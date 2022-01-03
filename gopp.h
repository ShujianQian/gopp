////////////////////////////////////////////////////////////////////////////////
/// \file gopp.h
///
/// \brief Interface of the gopp thread pool library.
////////////////////////////////////////////////////////////////////////////////

#ifndef GOPP_H
#define GOPP_H

#include <cassert>
#include <mutex>
#include <vector>

#include <sys/epoll.h>
#include <x86intrin.h>

struct ucontext;

namespace go {

/// \brief Basically a link list node. Represents a queue.
///
/// An empty queue(list) is represented by the head pointing to itself.
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

///
/// \brief Per-thread scheduler.
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
  /// \brief Ask the scheduler to run a different Routine.
  ///
  /// \param state The state that this Routine is in.
  /// \param q The sleep queue to add to if Routine is in State::SleepState.
  /// \param sleep_lock The mutex guarding the sleep queue.
  ///
  /// The Routine calling RunNext can be put into sleep, put on the ready queue,
  /// put on the ready queue to run next, terminated and cleaned.
  void RunNext(State state, Queue *q = nullptr, std::mutex *sleep_lock = nullptr);
  void CollectGarbage();
  /// \brief Add a Routine to the scheduler.
  ///
  /// \param r The routine to be added. If only to notify the scheduler, use
  ///          a nullptr.
  /// \param batch Whether to batch multiple Routine before notifying the
  ///              scheduler.
  void WakeUp(Routine *r = nullptr, bool batch = false);
  void WakeUp(Routine **routines, size_t nr_routines, bool batch = false);

  void StartRoutineStub();

  Routine *current_routine() const { return current; }
  int thread_pool_id() const { return pool_id; }

  EventSource *event_source(int idx) const { return sources[idx]; }

  static Scheduler *Current();
  /// \brief Get the id of the current thread that the scheduler is associated
  /// with.
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

  /// \brief Locks current scheduler with another scheduler.
  ///
  /// \param with The other scheduler to lock with. If nullptr passed, only lock
  ///             the current scheduler.
  ///
  /// The address is used for ordering to avoid deadlocks.
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

/// \brief Routines to be scheduled and executed in the thread pool.
///
///
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

  /// \brief Associates this routine with a scheduler.
  ///
  /// \param v The Scheduler to be associated with.
  void set_scheduler(Scheduler *v) { sched = v; }
  virtual void AddToReadyQueue(Scheduler::Queue *q, bool next_ready = false);
  virtual void OnRemoveFromReadyQueue() {}
  virtual void OnFinish() {}

  virtual void Run() = 0;
  /// \brief Wraps around the Run virtual function and exits after returning from
  /// the Run function call
  void Run0();

 protected:
  /// \brief Allocate a new stack and makes a context.
  ///
  /// \param sched Unused.
  void InitStack(Scheduler *sched);
  /// \brief Reuse stack and context from an existing routine.
  ///
  /// \param c The \ref ucontext to be reused.
  /// \param sched Unused.
  /// \param sp Pointer to the stack to be reused.
  void InitFromGarbageContext(ucontext *c, Scheduler *sched, void *sp);
};

static_assert(sizeof(Routine) % 64 == 0, "Cacheline should be aligned");

/// \brief Generic Routine class that takes a callable object and invokes it
///        during Run().
template <class T>
class GenericRoutine : public Routine {
  T obj;
 public:
  GenericRoutine(const T &rhs) : obj(rhs) {}

  virtual void Run() { obj.operator()(); }
};

/// \brief Creates a GenericRoutine object from a callable.
///
/// \tparam T Callable type.
/// \param obj Callable object.
/// \return GenericRoutine that invokes the callable during run.
template <class T>
Routine *Make(const T &obj)
{
  return new GenericRoutine<T>(obj);
}

/// \brief Creates and initializes a thread pool.
///
/// \param nr_threads Number of threads.
/// \param allocator Routine stack allocator. A default allocator is used when
///                  no allocator is provided.
void InitThreadPool(int nr_threads = 1, RoutineStackAllocator *allocator = nullptr);

///
/// \brief Signals the threads to finish and joins the thread pool.
void WaitThreadPool();

/// \brief Access the Scheduler associated with a particular thread.
///
/// \param thread_id ID of the thread that the Scheduler is associated with.
/// \return Pointer to the scheduler associated with the thread.
Scheduler *GetSchedulerFromPool(int thread_id);

class InputChannel {
 public:
  virtual bool Read(void *data, size_t cnt) = 0;

  // Advanced peeking API.
  virtual void BeginPeek() {}
  virtual size_t Peek(void *data, size_t cnt) { return 0; }
  virtual void Skip(size_t skip) {}
  virtual void EndPeek() {}
};

class OutputChannel {
 public:
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
