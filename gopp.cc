#include "gopp.h"
#include <cassert>
#include <cstdio>
#include <cstring>
#include <vector>
#include <unistd.h>
#include <pthread.h>
#include <cstdarg>
#include <atomic>
#include <thread>
#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <sys/time.h>

#include "channels.h"
#include "ucontext.h"

#if __has_feature(address_sanitizer)

extern "C" {
  void __sanitizer_start_switch_fiber(void **fake_stack_save, const void *bottom, size_t size);
  void __sanitizer_finish_switch_fiber(void *fake_stack_save, const void **bottom_old, size_t *old);
}

#endif

namespace go {

class ScheduleEventSource : public EventSource {
  int fd; // Only used for notification
  static int share_fd;
  static Scheduler::Queue share_q;
  static std::mutex share_m;
  Event events[2];
 public:
  ScheduleEventSource(Scheduler *sched);

  void OnEvent(Event *evt) final;
  bool ReactEvents() final;
  void SendEvents(Routine *r, bool notify = false);
  void SendEvents(Routine **routines, size_t nr_routines, bool notify);
 private:
  static void Initialize();
  friend void InitThreadPool(int, RoutineStackAllocator *);
  friend void WaitThreadPool();
};

void Routine::Run0()
{
  sched->mutex.unlock();

#if __has_feature(address_sanitizer)
  const void *__asan_old_stack_bottom;
  size_t __asan_old_stack_offset;
  __sanitizer_finish_switch_fiber(nullptr,
                                  &__asan_old_stack_bottom,
                                  &__asan_old_stack_offset);
#endif

  Run();
  sched->RunNext(Scheduler::ExitState);
}

static void routine_func(void *r)
{
  ((Routine *) r)->Run0();
}

const size_t RoutineStackAllocator::kContextSize = sizeof(struct ucontext);

void RoutineStackAllocator::AllocateStackAndContext(size_t &stack_size,
                                                    ucontext * &ctx_ptr, void * &stack_ptr)
{
  stack_size = kDefaultStackSize;
  ctx_ptr = (ucontext *) calloc(1, sizeof(struct ucontext));
  stack_ptr = malloc(kDefaultStackSize);
  // fprintf(stderr, "alloc new ctx %p stack %p\n", ctx_ptr, stack_ptr);
}

void RoutineStackAllocator::FreeStackAndContext(ucontext *ctx_ptr, void *stack_ptr)
{
  free(ctx_ptr);
  free(stack_ptr);
}

static RoutineStackAllocator *g_allocator;

void Routine::InitStack(Scheduler *sched)
{
  // ctx = (ucontext *) calloc(1, sizeof(ucontext_t));
  void *stack = nullptr;
  size_t stack_size = 0;
  g_allocator->AllocateStackAndContext(stack_size, ctx, stack);

  ctx->stack.stack_bottom = stack;
  ctx->stack.size = stack_size;;
  makecontext(ctx, routine_func, this);
}

void Routine::InitFromGarbageContext(struct ucontext *c, Scheduler *sched, void *sp)
{
  ctx = c;
  makecontext(ctx, routine_func, this);
}

//! A thread_local thread_pool_id used to get the scheduler associated with the
//! current thread.
static __thread int tls_thread_pool_id = -1;

//! Vector containing schedulers assiciated with \ref g_thread_pool.
static std::vector<Scheduler *> g_schedulers;

//! Signal that notifies threads in the thread pool to exit upon idling.
static std::atomic_bool g_thread_pool_should_exit(false);

Routine::Routine()
    : user_data(nullptr), reuse(false), urgent(false), share(false)
{
  Reset();
}

void Routine::Reset()
{
  Init();
  sched = nullptr;
  ctx = nullptr;
}

void Routine::AddToReadyQueue(Scheduler::Queue *q, bool next_ready)
{
  if (next_ready) {
    auto p = q->next;
    if (p == q || !((Routine *) p)->urgent) {
      Add(p);
    } else {
      while (p != q && ((Routine *) p)->urgent) p = p->next;
      Add(p->prev);
    }
  } else if (urgent) {
    // if urgent add to head of queue
    Add(q);
  } else {
    // otherwise, add to tail of queue
    Add(q->prev);
  }
}

void Routine::VoluntarilyPreempt(bool urgent)
{
  if (urgent)
    sched->RunNext(go::Scheduler::NextReadyState);
  else
    sched->RunNext(go::Scheduler::ReadyState);
}

Scheduler::Scheduler(Routine *r)
    : waiting(false), current(nullptr), prev_ctx(nullptr), delay_garbage_ctx(nullptr)
{
  ready_q.Init();
  epoll_fd = epoll_create(1);
  if (epoll_fd < 0) {
    perror("epoll_create");
    std::abort();
  }
  pool_id = -1;

  sources.push_back(new ScheduleEventSource(this));
  sources.push_back(new NetworkEventSource(this));

  idle = current = r;
  idle->sched = this;
}

Scheduler::~Scheduler()
{
  for (auto &event_source: sources) {
    delete event_source;
  }
  CollectGarbage();
}

// This is merely a stub on the call stack. Can only be invoked by the idle routine
void Scheduler::StartRoutineStub()
{
  setjmp(idle->ctx->mcontext);
}

Scheduler *Scheduler::Current()
{
  if (__builtin_expect(tls_thread_pool_id < 0, 0)) {
    fprintf(stderr, "Not a go-routine thread, return null on %s\n", __FUNCTION__);
    return nullptr;
  }
  auto res = g_schedulers[tls_thread_pool_id];
  if (__builtin_expect(res == nullptr, 0)) {
    fprintf(stderr, "This go-routine thread has not associate with a scheduler\n");
  }
  return res;
}

int Scheduler::CurrentThreadPoolId()
{
  return tls_thread_pool_id;
}

Scheduler *GetSchedulerFromPool(int thread_id)
{
  return g_schedulers[thread_id];
}

/// \brief Switch to run another Routine.
///
/// \param state End state of the current routine.
/// \param sleep_q Queue of sleeping routines.
/// \param sleep_lock Mutex guarding the sleep queue.
void Scheduler::RunNext(State state, Queue *sleep_q, std::mutex *sleep_lock)
{
  // fprintf(stderr, "[go] RunNext() on thread %d\n", tls_thread_pool_id);
  bool stack_reuse = false;
  bool should_delete_old = false;
  bool busy_poll = current->busy_poll;

  mutex.lock();

  CollectGarbage();

  if (__builtin_expect(!current->is_detached(), false) && state != ExitState) {
    fprintf(stderr, "[go] current %p must be detached!\n", current);
    std::abort();
  }

  struct ucontext *old_ctx = nullptr, *next_ctx = nullptr;
  Routine *old = current, *next = nullptr;

  if (state == SleepState) {
    // if this routine is set to sleep,
    // it will not be added back to the ready queue
    // instead, it will need to be explicitly wakeup before it can be scheduled
    // to run again
    if (sleep_q)
      old->Add(sleep_q->prev);
    if (sleep_lock)
      sleep_lock->unlock();
  } else if (state == ReadyState) {
    old->AddToReadyQueue(&ready_q);
  } else if (state == NextReadyState) {
    old->AddToReadyQueue(ready_q.next, true);
  } else if (state == ExitState) {
    if (current == idle) std::abort();
    should_delete_old = true;
    // delay collection of the old ucontext so that the next routine may be
    // able to reuse the ucontext of the old routine
    delay_garbage_ctx = old->ctx;
    // fprintf(stderr, "ctx %p is garbage now, stack bottom %p size %lu\n",
    //         delay_garbage_ctx, delay_garbage_ctx->stack.stack_bottom,
    //         delay_garbage_ctx->stack.size);
  }
  old_ctx = old->ctx;

  // if (state == ExitState) old->ctx = nullptr;
  if (should_delete_old) {
    if (old->reuse) old->OnFinish();
    else delete old;
  }

again:
  auto ent = ready_q.next;

  if (!busy_poll && ent != &ready_q) {
    // if not busy_poll and ready_q is not empty
    // then get the next ready routine
    next = (Routine *) ent;
    if (!next->ctx) {
      if (delay_garbage_ctx) {
	// reuse the stack and context memory
	// fprintf(stderr, "reuse ctx %p stack %p\n", delay_garbage_ctx, delay_garbage_ctx->stack.stack_bottom);
	next->InitFromGarbageContext(delay_garbage_ctx, this, delay_garbage_ctx->stack.stack_bottom);
	delay_garbage_ctx = nullptr;
	stack_reuse = true;
      } else {
	next->InitStack(this);
      }
    }
    next->Detach();
    next->OnRemoveFromReadyQueue();
  } else {
    // if busy_poll or ready_q is empty
    int timeout = -1;
    if (busy_poll && ent != &ready_q) {
      // if busy_poll and ready_q is not empty
      // do not block on epoll wait
      // also do not busy_poll again, i.e. poll once and run next routine
      timeout = 0;
      busy_poll = false;
    } else {
      // if ready_q is indeed empty
      // first react to events from all sources
      // and wait indefinitely on epoll_wait
      for (auto event_source : sources) {
        if (event_source->ReactEvents())
          // if should react to events again, go back
          goto again;
      }
    }
    waiting = true;
    mutex.unlock();
    // All effort to react previous handled events failed, we really need to
    // poll for new events
    // fprintf(stderr, "pool id %d epoll_wait()\n", tls_thread_pool_id);

 epoll_again:
    struct epoll_event kernel_events[kNrEpollKernelEvents];
    int rs = epoll_wait(epoll_fd, kernel_events, kNrEpollKernelEvents, timeout);
    if (rs < 0 && errno != EINTR) {
      perror("epoll");
      std::abort();
    }
    if (rs < 0 && errno == EINTR) {
      // if epoll_wait was interrupted by a signal handler
      // retry
      goto epoll_again;
    }
    // if event received, event source should react to the event
    for (int i = 0; i < rs; i++) {
      Event *e = (Event *) kernel_events[i].data.ptr;
      e->mask = kernel_events[i].events;
      sources[e->event_source_type]->OnEvent(e);
    }

    // fprintf(stderr, "pool id %d epoll_awake. %d events\n", tls_thread_pool_id, rs);
    mutex.lock();
    waiting = false;

    goto again;
  }

  // Switch context to run the next routine

  // switch scheduler states
  next_ctx = next->ctx;
  prev_ctx = stack_reuse ? nullptr : old_ctx;
  current = next;

#if __has_feature(address_sanitizer)
  void *fake_stack;
  __sanitizer_start_switch_fiber(&fake_stack,
                                 next_ctx->stack.stack_bottom,
                                 next_ctx->stack.size);
#endif

  // context switch
  if (stack_reuse) {
    // puts("Fast switch");
    swapcontext(nullptr, next_ctx);
  } else if (old_ctx != next_ctx) {
    swapcontext(old_ctx, next_ctx);

#if __has_feature(address_sanitizer)
    const void *__asan_old_stack_bottom;
    size_t __asan_old_stack_offset;
    __sanitizer_finish_switch_fiber(fake_stack,
                                    (const void **) &__asan_old_stack_bottom,
                                    &__asan_old_stack_offset);
#endif
  }
  // if stack is not reused, will continue execution from here upon resuming
  Scheduler::Current()->mutex.unlock();
}

void Scheduler::WakeUp(Routine *r, bool batch)
{
  // if (r) r->sched = this;
  ((ScheduleEventSource *) sources[ScheduleEventSourceType])->SendEvents(r, !batch);
}

void Scheduler::WakeUp(Routine **routines, size_t nr_routines, bool batch)
{
  ((ScheduleEventSource *) sources[ScheduleEventSourceType])->SendEvents(routines, nr_routines, !batch);
}

void Scheduler::CollectGarbage()
{
  if (delay_garbage_ctx) {
    // fprintf(stderr, "Collecting %p stack %p\n",
    //         delay_garbage_ctx, delay_garbage_ctx->stack.stack_bottom);
    g_allocator->FreeStackAndContext(delay_garbage_ctx, delay_garbage_ctx->stack.stack_bottom);
    delay_garbage_ctx = nullptr;
  }
}

//! Thread pool vector containing std::thread objects.
static std::vector<std::thread> g_thread_pool;

// Schedule Events
ScheduleEventSource::ScheduleEventSource(Scheduler *sched)
    : EventSource(sched), fd(eventfd(0, EFD_NONBLOCK)),
      events{Event(fd, ScheduleEventSourceType), Event(share_fd, ScheduleEventSourceType)}
{
  struct epoll_event kernel_event = {
    EPOLLIN, {&events[0]},
  };
  struct epoll_event share_kernel_event = {
    EPOLLIN, {&events[1]},
  };
  if (epoll_ctl(sched_epoll(), EPOLL_CTL_ADD, fd, &kernel_event) < 0
      || epoll_ctl(sched_epoll(), EPOLL_CTL_ADD, share_fd, &share_kernel_event) < 0) {
    perror("epoll ctr");
    std::abort();
  }
}

void ScheduleEventSource::OnEvent(Event *evt)
{
  uint64_t p = 0;
  while (true) {
    if (read(evt->fd, &p, sizeof(uint64_t)) < 0) {
      if (errno == EINTR) continue;
      if (errno == EWOULDBLOCK || errno == EAGAIN) break;
    }
  }
}

bool ScheduleEventSource::ReactEvents()
{
  Routine *r = nullptr;
  ScheduleEntity *ent;

  std::unique_lock<std::mutex> _(share_m);

  // if pool is signaled to exit when run the idle routine
  if (g_thread_pool_should_exit.load())
    goto run_idle;

  ent = share_q.next;
  // if shared_queue is empty
  if (ent == &share_q)
    goto run_idle;

  // if shared_queue is not empty, add it to the ready queue of this scheduler
  r = (Routine *) ent;
  r->Detach();
  r->set_scheduler(sched);
  r->AddToReadyQueue(sched_ready_queue());
  return true;

run_idle:
  // if current thread is not already idle_routine
  // and the thread_pool is signaled to exit,
  // add idle routine to the ready queue
  if (sched_current() != sched_idle_routine() || g_thread_pool_should_exit.load()) {
    sched_idle_routine()->AddToReadyQueue(sched_ready_queue());
    return true;
  } else {
    // if the current routine is already idle routine
    // and the thread_pool is not signaled to exit,
    // do not react
    return false;
  }
}

///
/// \brief Sends a Routine to this scheduler or to the shared queue. Notify.
///
/// \param r Pointer to the Routine to send.
/// \param notify Whether to notify by writing to the fd.
void ScheduleEventSource::SendEvents(Routine *r, bool notify)
{
  uint64_t u = 1;

  // if only to notify, directly writes to fd
  if (r == nullptr && notify) {
    LockScheduler(nullptr);
    if (sched_is_waiting()) {
      if (write(fd, &u, sizeof(uint64_t)) < sizeof(uint64_t)) {
        perror("write to event fd");
        std::abort();
      }
    }
    UnlockScheduler(nullptr);
    return;
  }

  // detach from the old_scheduler
  auto old_sched = r->scheduler();
  LockScheduler(old_sched);
  r->Detach();
  r->set_scheduler(sched);

  if (r->is_share()) {
    // if r is a shared routine add it to the shared queue
    std::lock_guard<std::mutex> _(share_m);
    r->AddToReadyQueue(&share_q);

    if (notify) {
      if (write(share_fd, &u, sizeof(uint64_t)) < sizeof(uint64_t)) {
        perror("write to share event fd");
        std::abort();
      }
    }
  } else {
    // otherwise, add to current scheduler's ready queue
    r->AddToReadyQueue(sched_ready_queue());

    if (notify && sched_is_waiting()) {
      if (write(fd, &u, sizeof(uint64_t)) < sizeof(uint64_t)) {
        perror("write to event fd");
        std::abort();
      }
    }
  }
  UnlockScheduler(old_sched);
}

void ScheduleEventSource::SendEvents(Routine **routines, size_t nr_routines, bool notify)
{
  for (int i = 0; i < nr_routines; i++) {
    if (routines[i]->scheduler() != nullptr || routines[i]->is_share()) {
      fprintf(stderr, "Cannot add in batch when some routines belong to other schedulers\n");
      std::abort();
    }
  }
  LockScheduler(nullptr);

  for (int i = 0; i < nr_routines; i++) {
    routines[i]->set_scheduler(sched);
    routines[i]->AddToReadyQueue(sched_ready_queue());
  }

  uint64_t u = 0;

  if (notify && sched_is_waiting()) {
    if (write(fd, &u, sizeof(uint64_t)) < sizeof(uint64_t)) {
      perror("write to event fd");
      std::abort();
    }
  }

  UnlockScheduler(nullptr);
}

int ScheduleEventSource::share_fd;

//! Shared ready queue.
Scheduler::Queue ScheduleEventSource::share_q;

//! Mutex guarding the shared ready queue.
std::mutex ScheduleEventSource::share_m;

void ScheduleEventSource::Initialize()
{
  share_q.Init();
  share_fd = eventfd(0, EFD_NONBLOCK);
}

// create a "System Idle Process" for scheduler

class IdleRoutine : public Routine {
 public:
  IdleRoutine();
  void Run() final;
};

IdleRoutine::IdleRoutine()
{
  ctx = (struct ucontext *) calloc(1, sizeof(struct ucontext)); // just a dummy context, no make context
}

void IdleRoutine::Run()
{
  while (!g_thread_pool_should_exit.load()) {
    Scheduler::Current()->RunNext(Scheduler::SleepState);
  }
}

/// Creates and initializes a thread pool.
///
/// The initialization includes:
///  * Creates a default allocator if no allocator is provided.
///  * Creates nr_threads std::threads.
void InitThreadPool(int nr_threads, RoutineStackAllocator *allocator)
{
  g_allocator = (allocator == nullptr)
                ? new RoutineStackAllocator() : allocator;

  ScheduleEventSource::Initialize();

  // number of threads that have finished setup
  std::atomic<int> nr_up(0);
  g_schedulers.resize(nr_threads + 1, nullptr);

  for (int i = 0; i <= nr_threads; i++) {
    g_thread_pool.emplace_back(std::thread([&nr_up, i, nr_threads] {
	  // linux only
	  cpu_set_t set;
          int nr_processors = sysconf(_SC_NPROCESSORS_CONF);
	  CPU_ZERO(&set);
	  if (nr_processors >= i && i > 0) {
	    CPU_SET(i - 1 , &set);
	  } else {
	    for (int j = 0; j < nr_threads; j++) CPU_SET(j % nr_processors, &set);
	  }
	  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &set);

      // creates a detached idle routine
	  auto idle_routine = new IdleRoutine;
	  idle_routine->Detach();

          // creates a scheduler and associates with the current thread
          auto sched = new Scheduler(idle_routine);
          sched->pool_id = i;
          tls_thread_pool_id = i;
          g_schedulers[tls_thread_pool_id] = sched;

      // thread setup finished, increment up counter
	  nr_up.fetch_add(1);

          sched->StartRoutineStub();  // sets context for the idle routine
                                      // will jump here when idle
	  idle_routine->Run();
	}));
  }

  // spin until all threads have finished setup
  while (nr_up.load() <= nr_threads);
}

void WaitThreadPool()
{
  g_thread_pool_should_exit.store(true);

  for (auto sched: g_schedulers) {
    int fd = ((ScheduleEventSource *) sched->sources[0])->fd;
    uint64_t u = 1;
    if (write(fd, &u, sizeof(uint64_t)) < sizeof(uint64_t)) {
      fputs("warning: cannot signal the thread via event fd\n", stderr);
    }
  }

  for (auto &t: g_thread_pool) {
    t.join();
  }

  for (auto sched: g_schedulers) {
    free(sched->idle->ctx);
    delete sched->idle;
    delete sched;
  }
}

}
