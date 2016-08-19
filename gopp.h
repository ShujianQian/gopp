// -*- c++ -*-

#ifndef GOPP_H
#define GOPP_H

#include <cstddef>
#include <cstdlib>
#include <cassert>
#include <limits.h>
#include <queue>
#include <array>
#include <functional>
#include <mutex>
#include <map>
#include <condition_variable>

#include "amd64-ucontext.h"

namespace go {

// basically a link list node
struct ScheduleEntity {
  ScheduleEntity *prev, *next;

  void Add(ScheduleEntity *parent) {
    this->prev = parent;
    this->next = parent->next;
    parent->next->prev = this;
    parent->next = this;
  }
  void Detach() {
    prev->next = next;
    next->prev = prev;
    next = prev = nullptr;
  }
  void Init() {
    prev = next = this;
  }
  bool is_detached() const { return next == nullptr; }
};

class Routine;
class OptionalMutex;

class Scheduler {
public:
  typedef ScheduleEntity Queue;
private:
  std::mutex mutex;
  std::condition_variable cond;
  size_t resp_count;

  Queue ready_q;
  Routine *current, *delay_garbage;
  ucontext_t host_ctx;

public:
  Scheduler();
  ~Scheduler();

  enum State {
    ReadyState,
    SleepState,
    ExitState,
  };
  void RunNext(State state, Queue *q = nullptr, OptionalMutex *sleep_lock = nullptr);
  void CollectGarbage();
  void WakeUp(Routine *r, bool batch = false);

  void Signal() { cond.notify_one(); }

  Routine *current_routine() const { return current; }

  static Scheduler *Current();
  static int CurrentThreadPoolId();
  static void RegisterScheduler(int thread_id);
  static void UnRegisterScheduler();
};

class Routine : public ScheduleEntity {
  ucontext_t *ctx;
  Scheduler *sched;
  size_t w_delta;
  bool reuse;
friend Scheduler;
public:
  static const size_t kStackSize = (1UL << 20);

  Routine();
  virtual ~Routine() {}

  Routine(const Routine &rhs) = delete;
  Routine(Routine &&rhs) = delete;

  void Reset();

  void Run0() {
    Run();
    Scheduler::Current()->RunNext(Scheduler::ExitState);
  }

  void StartOn(int thread_id) {
    assert(sched == nullptr);
    assert(thread_id > 0);
    WakeUpOn(thread_id);
  }
  void WakeUp() { WakeUpOn(0); }

  void WakeUpOn(int thread_id);

  size_t wait_for_delta() const { return w_delta; }
  void set_wait_for_delta(size_t sz) { w_delta = sz; }
  void set_reuse(bool r) { reuse = r; }
  bool is_reuse() const { return reuse; }

protected:
  void InitStack(ucontext_t *link, size_t stack_size);
  virtual void Run() = 0;
};

template <class T>
class GenericRoutine : public Routine {
  T obj;
public:
  GenericRoutine(const T &rhs) : obj(rhs) {}

  virtual void Run() { obj.operator()(); }
};

template <class T>
Routine *Make(const T &obj)
{
  return new GenericRoutine<T>(obj);
}

void InitThreadPool(int nr_threads = 1);
void WaitThreadPool();

void RunOnMainThread();

Scheduler *GetSchedulerFromPool(int thread_id);

class SourceConditionVariable {
  Scheduler::Queue sleep_q;
  size_t cap;
public:
  SourceConditionVariable() : cap(0) {
    sleep_q.Init();
  }
  SourceConditionVariable(const SourceConditionVariable &rhs) = delete;
  SourceConditionVariable(SourceConditionVariable &&rhs) = delete;

  void WaitForSize(size_t size, OptionalMutex *lock);
  void Notify(size_t new_cap);

  size_t capacity() const { return cap; }
};

class OptionalMutex {
  std::mutex m;
  bool enabled = true;
public:
  void lock() {
    if (enabled) m.lock();
  }
  void unlock() {
    if (enabled) m.unlock();
  }
  OptionalMutex *mutex_ptr() {
    if (enabled) return this;
    else return nullptr;
  }
  void Enable() { enabled = true; }
  void Disable() { enabled = false; }
};

class DummyChannel {}; // no virtual table, use if you prefer template style Channel

template <typename T>
class InputChannel { // has virtual table, use if you prefer virtual style Channel
public:
  virtual ~InputChannel() {}
  virtual bool AcquireReadSpace(size_t size) = 0;
  virtual void EndRead(size_t size) = 0;
  virtual T ReadOne() = 0;
  virtual void ReadAll(T *buf, size_t cnt) {
    for (int i = 0; i < cnt; i++) buf[i] = ReadOne();
  }
  virtual T Read(bool &eof) = 0;
};

template <typename T>
class OutputChannel {
public:
  virtual ~OutputChannel() {}
  virtual void AcquireWriteSpace(size_t size) = 0;
  virtual void EndWrite(size_t size) = 0;
  virtual void WriteOne(const T &rhs) = 0;
  virtual void WriteAll(const T *buf, size_t cnt) {
    for (int i = 0; i < cnt; i++) WriteOne(buf[i]);
  }
  virtual void Write(const T &rhs) = 0;
  virtual void Close() = 0;
  virtual void Flush() = 0;
};

template <class T>
class InputOutputChannel : public InputChannel<T>, public OutputChannel<T> {};

// BaseClass could either be DummyChannel or InputOutputChannel
template <typename T, class Container, class BaseClass>
class BaseBufferChannel : public BaseClass {
  SourceConditionVariable read_cv, write_cv;
  Container queue;
  OptionalMutex mutex;
  size_t limit;
  bool closed;
public:
  BaseBufferChannel(size_t lmt) : limit(lmt), closed(false) {}

  OptionalMutex &buffer_mutex() { return mutex; }

  bool AcquireReadSpace(size_t size) {
    if (size > limit && limit > 0) {
      throw std::invalid_argument("size larger than limit");
    }
    mutex.lock();
    while (queue.size() < size) {
      if (closed) return false;
      read_cv.WaitForSize(size, mutex.mutex_ptr());
    }
    return true;
  }

  void AcquireWriteSpace(size_t size) {
    if (size > limit && limit > 0) {
      throw std::invalid_argument("size larger than limit");
    }
    mutex.lock();
    if (limit > 0) {
      while (limit - queue.size() < size)
	write_cv.WaitForSize(size, mutex.mutex_ptr());
    }
  }
  void EndRead(size_t size) { mutex.unlock(); }
  void EndWrite(size_t size) {
    if (limit == 0) {
      // synchronous
      write_cv.WaitForSize(size, mutex.mutex_ptr());
    }
    mutex.unlock();
  }

  void WriteOne(const T &rhs) {
    queue.push(rhs);
    read_cv.Notify(queue.size());
  }
  T ReadOne() {
    T result(queue.front());
    queue.pop();
    if (limit > 0)
      write_cv.Notify(limit - queue.size());
    else
      write_cv.Notify(write_cv.capacity() - queue.size());
    return result;
  }
  void ReadAll(T *buf, size_t cnt) {
    for (int i = 0; i < cnt; i++) buf[i] = ReadOne();
  }
  void WriteAll(const T *buf, size_t cnt) {
    for (int i = 0; i < cnt; i++) WriteOne(buf[i]);
  }

  void Flush() {
    if (limit > 0) {
      std::lock_guard<OptionalMutex> _(mutex);
      while (!queue.empty())
	write_cv.WaitForSize(0, mutex.mutex_ptr());
    }
  }

  void Close() {
    std::lock_guard<OptionalMutex> _(mutex);
    closed = true;
    read_cv.Notify(LONG_MAX);
  }
};

template <typename T, class BaseClass>
class InputChannelWrapper : public BaseClass {
public:
  using BaseClass::BaseClass;

  T Read(bool &eof) {
    eof = !this->AcquireReadSpace(1);
    if (eof) {
      this->EndRead(1);
      return T();
    }
    T t = this->ReadOne();
    this->EndRead(1);
    return t;
  }

  bool Read(T *buf, size_t cnt = 1) {
    bool eof = !this->AcquireReadSpace(cnt);
    if (eof) return false;
    this->ReadAll(buf, cnt);
    this->EndRead(cnt);
    return true;
  }

  bool Read(void *raw, size_t cnt) {
    assert(cnt % sizeof(T) == 0);
    return Read((T *) raw, cnt / sizeof(T));
  }
};

template <typename T, class BaseClass>
class OutputChannelWrapper : public BaseClass {
public:
  using BaseClass::BaseClass;

  void Write(const T &rhs) {
    this->AcquireWriteSpace(1);
    this->WriteOne(rhs);
    this->EndWrite(1);
  }

  void Write(const T *buf, size_t cnt = 1) {
    this->AcquireWriteSpace(cnt);
    this->WriteAll(buf, cnt);
    this->EndWrite(cnt);
  }

  void Write(const void *raw, size_t cnt) {
    assert(cnt % sizeof(T) == 0);
    Write((T *) raw, cnt / sizeof(T));
  }
};

template <typename T, class BaseClass>
class InputOutputChannelWrapper : public BaseClass {
public:
  using BaseClass::BaseClass;
};

template <typename T, class Container = std::queue<T>, class BaseClass = DummyChannel>
using BufferChannel = InputOutputChannelWrapper<T, InputChannelWrapper<T, OutputChannelWrapper<T, BaseBufferChannel<T, Container, BaseClass> > > >;

}

#endif /* GOPP_H */
