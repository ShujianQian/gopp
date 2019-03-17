// -*- c++ -*-

#ifndef GOPP_H
#define GOPP_H

#include <cassert>
#include <mutex>
#include <vector>

#include <sys/epoll.h>
#include <x86intrin.h>

struct ucontext;

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

class RoutineStackAllocator {
 public:
  static const size_t kDefaultStackSize = (8UL << 20);
  static const size_t kContextSize;

  virtual void AllocateStackAndContext(size_t &stack_size, ucontext * &ctx_ptr, void * &stack_ptr);
  virtual void FreeStackAndContext(ucontext *ctx_ptr, void *stack_ptr);
};

class Scheduler {
 public:
  typedef ScheduleEntity Queue;
 private:
  friend class Routine;
  friend class EventSource;

  friend void InitThreadPool(int, RoutineStackAllocator *);
  friend void WaitThreadPool();

  std::mutex mutex;
  bool waiting;
  std::vector<EventSource*> sources;
  Queue ready_q;

  Routine *current;
  ucontext *prev_ctx;
  ucontext *delay_garbage_ctx;
  Routine *idle;

  long link_rip, link_rbp;

  static const int kNrEpollKernelEvents = 32;
  int epoll_fd;
  int pool_id;

  unsigned char __padding__[48];

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
  void WakeUp(Routine *r = nullptr, bool batch = false);
  void WakeUp(Routine **routines, size_t nr_routines, bool batch = false);

  void StartRoutineStub();

  Routine *current_routine() const { return current; }
  int thread_pool_id() const { return pool_id; }

  EventSource *event_source(int idx) const { return sources[idx]; }

  static Scheduler *Current();
  static int CurrentThreadPoolId();
};

static_assert(sizeof(Scheduler) % 64 == 0, "Must aligned to Cacheline");

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
  ucontext *ctx;
  Scheduler *sched;
  void *user_data;
  bool reuse;
  bool urgent;
  bool share;
  bool busy_poll;
  unsigned char __padding__[8];

  friend class Scheduler;
  friend void InitThreadPool(int);
  friend void WaitThreadPool();
 public:

  Routine();
  virtual ~Routine() {}

  Routine(const Routine &rhs) = delete;
  Routine(Routine &&rhs) = delete;

  void Reset();

  void VoluntarilyPreempt(bool urgent);
  void set_reuse(bool r) { reuse = r; }
  void set_share(bool s) { share = s; }
  void set_urgent(bool u) { urgent = u; }
  bool is_urgent() const { return urgent; }
  void set_busy_poll(bool p) { busy_poll = p; }
  bool is_share() const { return share; }
  void *userdata() const { return user_data; }
  void set_userdata(void *p) { user_data = p; }

  Scheduler *scheduler() const { return sched; }

  // internal use
  void set_scheduler(Scheduler *v) { sched = v; }
  virtual void AddToReadyQueue(Scheduler::Queue *q, bool next_ready = false);
  virtual void OnRemoveFromReadyQueue() {}

  virtual void Run() = 0;
  void Run0();

 protected:
  void InitStack(Scheduler *sched);
  void InitFromGarbageContext(ucontext *c, Scheduler *sched, void *sp);
};

static_assert(sizeof(Routine) % 64 == 0, "Cacheline should be aligned");

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

void InitThreadPool(int nr_threads = 1, RoutineStackAllocator *allocator = nullptr);
void WaitThreadPool();

Scheduler *GetSchedulerFromPool(int thread_id);

class InputChannel {
  virtual size_t Poll() = 0;
  virtual bool Read(void *data, size_t cnt) = 0;
};

class OutputChannel {
  virtual bool Write(const void *data, size_t cnt) = 0;
  // Wait until everything in the buffer is written.
  virtual void Flush(bool async = false) = 0;
  virtual void Close() = 0;
};


class RoutineScopedData {
  void *olddata;
 public:
  RoutineScopedData(void *data) {
    auto r = go::Scheduler::Current()->current_routine();
    olddata = r->userdata();
    r->set_userdata(data);
  }
  ~RoutineScopedData() {
    go::Scheduler::Current()->current_routine()->set_userdata(olddata);
  }
};

}

#endif /* GOPP_H */
