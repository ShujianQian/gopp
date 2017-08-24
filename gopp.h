// -*- c++ -*-

#ifndef GOPP_H
#define GOPP_H

#include <cstddef>
#include <cstdlib>
#include <cassert>
#include <climits>
#include <functional>
#include <mutex>
#include <vector>

#include <sys/epoll.h>

#include <x86intrin.h>

#include "amd64-ucontext.h"

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

enum EventSourceTypes : int {
  ScheduleEventSourceType,
  NetworkEventSourceType,
};

struct Event {
  int fd;
  int mask;
  int event_source_type;
  Event(int fd, int source_type) : fd(fd), event_source_type(source_type) {}
  virtual ~Event() {}
};

class EventSource;

class Scheduler {
 public:
  typedef ScheduleEntity Queue;
 private:
  friend class Routine;
  friend class EventSource;

  friend void InitThreadPool(int);
  friend void WaitThreadPool();

  std::mutex mutex;
  bool waiting;
  std::vector<EventSource*> sources;
  Queue ready_q;

  Routine *current;
  Routine *previous;
  ucontext_t *delay_garbage_ctx;
  Routine *idle;

  static const int kNrEpollKernelEvents = 32;
  int epoll_fd;
  struct epoll_event kernel_events[kNrEpollKernelEvents];

  Scheduler(Routine *r);
  ~Scheduler();
 public:
  enum State {
    ReadyState,
    NextReadyState,
    SleepState,
    ExitState,
  };
  void RunNext(State state, Queue *q = nullptr, std::mutex *sleep_lock = nullptr);
  void CollectGarbage();
  void WakeUp(Routine *r, bool batch = false);

  Routine *current_routine() const { return current; }

  EventSource *event_source(int idx) const { return sources[idx]; }

  static Scheduler *Current();
  static int CurrentThreadPoolId();
};

class EventSource {
 protected:
  Scheduler *sched;
 public:
  EventSource(Scheduler *sched) : sched(sched) {}
  virtual ~EventSource() {}

  virtual void OnEvent(Event *evt) = 0;
  virtual bool ReactEvents() = 0;
 protected:
  Scheduler::Queue *sched_ready_queue() { return &sched->ready_q; }
  int sched_epoll() const { return sched->epoll_fd; }
  Routine *sched_idle_routine() { return sched->idle; }
  Routine *sched_current() { return sched->current; }
  bool sched_is_waiting() const { return sched->waiting; }

  void LockScheduler(Scheduler *with = nullptr) {
    if (with && with < sched) with->mutex.lock();
    sched->mutex.lock();
    if (with && with > sched) with->mutex.lock();
  }
  void UnlockScheduler(Scheduler *with = nullptr) {
    if (with && with != sched) with->mutex.unlock();
    sched->mutex.unlock();
  }
};

class Routine : public ScheduleEntity {
 protected:
  ucontext_t *ctx;
  Scheduler *sched;
  void *user_data;
  bool reuse;
  bool urgent;
  bool share;

  friend class Scheduler;
  friend void InitThreadPool(int);
 public:

  static const size_t kStackSize = (8UL << 20);

  Routine();
  virtual ~Routine() {}

  Routine(const Routine &rhs) = delete;
  Routine(Routine &&rhs) = delete;

  void Reset();

  void VoluntarilyPreempt(bool urgent);
  void set_reuse(bool r) { reuse = r; }
  void set_share(bool s) { share = s; }
  bool is_share() const { return share; }
  void *userdata() const { return user_data; }
  void set_userdata(void *p) { user_data = p; }

  // internal use
  Scheduler *scheduler() const { return sched; }
  void set_scheduler(Scheduler *v) { sched = v; }
  void AddToReadyQueue(Scheduler::Queue *q) {
    if (urgent)
      Add(q);
    else
      Add(q->prev);
  }

  virtual void Run() = 0;
  void Run0();

 protected:
  void InitStack(ucontext_t *link, size_t stack_size);
  void InitFromGarbageContext(ucontext_t *ctx, ucontext_t *link, void *sp);
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

Scheduler *GetSchedulerFromPool(int thread_id);

class InputChannel {
  virtual bool Read(void *data, size_t cnt) = 0;
};

class OutputChannel {
  virtual bool Write(const void *data, size_t cnt) = 0;
  // Wait until everything in the buffer is written.
  virtual void Flush() = 0;
  virtual void Close() = 0;
};

}

#endif /* GOPP_H */
