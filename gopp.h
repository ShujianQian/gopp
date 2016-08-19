// -*- c++ -*-

// TODO: need to decouple the scheduler, synchronizations, and the channel into different files
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

#include <x86intrin.h>

#include "ucontext.h"

namespace go {

// basically a link list node
struct ScheduleEntity {
  ScheduleEntity *prev, *next;

  void Add(ScheduleEntity *parent) {
    prev = parent;
    next = parent->next;
    next->prev = this;
    prev->next = this;
  }
  void Detach() {
    prev->next = next;
    next->prev = prev;
    next = prev = nullptr;
  }
  void Init() {
    prev = next = this;
  }
  bool is_detached() const { return prev == next && next == nullptr; }
};

class Routine;
class OptionalMutex;

class Scheduler {
public:
  typedef ScheduleEntity Queue;
private:
friend Routine;
  std::mutex mutex;
  std::condition_variable cond;

  Queue ready_q;

  Routine *current;
  ucontext_t *delay_garbage_ctx;
  Routine *idle;

  uint64_t stack_ver;

  static Queue share_q;
  static std::mutex share_m;

public:
  Scheduler();
  ~Scheduler();

  enum State {
    ReadyState,
    NextReadyState,
    SleepState,
    ExitState,
  };
  void RunNext(State state, Queue *q = nullptr, OptionalMutex *sleep_lock = nullptr);
  void BottomHalf(Routine *r);
  void CollectGarbage();
  void WakeUp(Routine *r, bool batch = false);

  Routine *current_routine() const { return current; }
  void Init(Routine *idle);

  static void InitShareQueue();

  static Scheduler *Current();
  static int CurrentThreadPoolId();
  static void RegisterScheduler(int thread_id);
  static void UnRegisterScheduler();
private:

friend void WaitThreadPool();
};

class Routine : public ScheduleEntity {
protected:
  ucontext_t *ctx;
  Scheduler *sched;
  size_t w_delta;
  bool reuse;
  bool urgent;
  bool share;
  uint64_t stack_ver;

friend Scheduler;
public:

  static const size_t kStackSize = (1UL << 20);

  Routine();
  virtual ~Routine() {}

  Routine(const Routine &rhs) = delete;
  Routine(Routine &&rhs) = delete;

  void Reset();

  void StartOn(int thread_id) {
    assert(sched == nullptr);
    assert(thread_id >= 0);
    WakeUpOn(thread_id);
  }
  void WakeUp() { WakeUpOn(0); }

  void WakeUpOn(int thread_id);
  static Scheduler *FindSleepingScheduler();

  void VoluntarilyPreempt(bool force = true);

  size_t wait_for_delta() const { return w_delta; }
  void set_wait_for_delta(size_t sz) { w_delta = sz; }
  void set_reuse(bool r) { reuse = r; }
  void set_urgent(bool u) { urgent = u; }
  void set_share(bool s) { share = s; }

  virtual void Run() = 0;
  void Run0();

protected:
  void InitStack(ucontext_t *link, size_t stack_size);
  void InitFromGarbageContext(ucontext_t *ctx, void *sp);
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
// you're responsible for your concurrency controls. This force every thread exists peacefully.
void WaitThreadPool();

void RunOnMainThread();

Scheduler *GetSchedulerFromPool(int thread_id);

// Condition Variable like synchronization
class WaitSlot {
  Scheduler::Queue sleep_q;
  size_t cap;
public:
  WaitSlot() : cap(0) {
    sleep_q.Init();
  }
  WaitSlot(const WaitSlot &rhs) = delete;
  WaitSlot(WaitSlot &&rhs) = delete;

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
  bool try_lock() {
    if (enabled) return m.try_lock();
    return true;
  }
  OptionalMutex *mutex_ptr() {
    if (enabled) return this;
    else return nullptr;
  }
  void Enable() { enabled = true; }
  void Disable() { enabled = false; }
};

class WaitBarrier {
  OptionalMutex m;
  long counter;
  WaitSlot slot;
public:
  WaitBarrier(long max_waiter) : counter(max_waiter) {}
  void Wait() {
    std::lock_guard<OptionalMutex> _(m);
    if (--counter == 0) {
      slot.Notify(0);
    } else {
      slot.WaitForSize(0, &m);
    }
  }
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
  WaitSlot read_cv, write_cv;
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
