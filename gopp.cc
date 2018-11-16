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
#include "amd64-ucontext.h"

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
 private:
  static void Initialize();
  friend void InitThreadPool(int);
  friend void WaitThreadPool();
};

void Routine::Run0()
{
#if __has_feature(address_sanitizer)
  if (sched->prev_ctx) {
    puts("finishing switch fiber");
    __sanitizer_finish_switch_fiber(ctx->asan_fake_stack,
                                    (const void **) &sched->prev_ctx->uc_stack.ss_sp,
                                    &sched->prev_ctx->uc_stack.ss_size);
  }
#endif
  sched->mutex.unlock();

  Run();
  sched->RunNext(Scheduler::ExitState);
}

static void routine_func(void *r)
{
  ((Routine *) r)->Run0();
}

void Routine::InitStack(Scheduler *sched, size_t stack_size)
{
  ctx = (ucontext *) calloc(1, sizeof(ucontext_t));
  ctx->uc_stack.ss_sp = malloc(stack_size);
  ctx->uc_stack.ss_size = stack_size;
  ctx->uc_stack.ss_flags = 0;
  ctx->uc_mcontext.mc_rip = sched->link_rip;
  ctx->uc_mcontext.mc_rbp = sched->link_rbp;

  makecontext(ctx, routine_func, this);
}

void Routine::InitFromGarbageContext(ucontext_t *c, Scheduler *sched, void *sp)
{
  ctx = c;
  ctx->uc_mcontext.mc_rip = sched->link_rip;
  ctx->uc_mcontext.mc_rbp = sched->link_rbp;
  makecontext(ctx, routine_func, this);
}

static __thread int tls_thread_pool_id = -1;
static std::vector<Scheduler *> g_schedulers;
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
  getmcontext(&idle->ctx->uc_mcontext);
  link_rip = idle->ctx->uc_mcontext.mc_rip;
  link_rbp = idle->ctx->uc_mcontext.mc_rbp;
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

void Scheduler::RunNext(State state, Queue *sleep_q, std::mutex *sleep_lock)
{
  // fprintf(stderr, "[go] RunNext() on thread %d\n", tls_thread_pool_id);
  bool stack_reuse = false;
  bool should_delete_old = false;

  mutex.lock();

  CollectGarbage();

  if (__builtin_expect(!current->is_detached(), false) && state != ExitState) {
    fprintf(stderr, "[go] current %p must be detached!\n", current);
    std::abort();
  }

  ucontext_t *old_ctx = nullptr, *next_ctx = nullptr;
  Routine *old = current, *next = nullptr;

  if (state == SleepState) {
    if (sleep_q)
      old->Add(sleep_q->prev);
    if (sleep_lock)
      sleep_lock->unlock();
  } else if (state == ReadyState) {
    old->Add(ready_q.prev);
  } else if (state == NextReadyState) {
    old->Add(ready_q.next);
  } else if (state == ExitState) {
    if (current == idle) std::abort();
    if (!old->reuse) {
      should_delete_old = true;
    }
    delay_garbage_ctx = old->ctx;
    // fprintf(stderr, "ctx %p is garbage now, ss_sp %p\n", delay_garbage_ctx, delay_garbage_ctx->uc_stack.ss_sp);
  }
  old_ctx = old->ctx;

  // if (state == ExitState) old->ctx = nullptr;
  if (should_delete_old) delete old;

again:
  auto ent = ready_q.next;

  if (ent != &ready_q) {
    next = (Routine *) ent;
    if (!next->ctx) {
#if __has_feature(address_sanitizer)
      next->InitStack(this, Routine::kStackSize);
#else
      if (delay_garbage_ctx) {
	// reuse the stack and context memory
	// fprintf(stderr, "reuse ctx %p stack %p\n", delay_garbage_ctx, delay_garbage_ctx->uc_stack.ss_sp);
	next->InitFromGarbageContext(delay_garbage_ctx, this, delay_garbage_ctx->uc_stack.ss_sp);
	delay_garbage_ctx = nullptr;
	stack_reuse = true;
      } else {
	next->InitStack(this, Routine::kStackSize);
      }
#endif
    }
    next->Detach();
  } else {
    for (auto event_source: sources) {
      if (event_source->ReactEvents()) goto again;
    }
    waiting = true;
    mutex.unlock();
    // All effort to react previous handled events failed, we really need to
    // poll for new events
    // fprintf(stderr, "epoll_wait()\n");
 epoll_again:
    int rs = epoll_wait(epoll_fd, kernel_events, kNrEpollKernelEvents, -1);
    if (rs < 0 && errno != EINTR) {
      perror("epoll");
      std::abort();
    }
    if (rs < 0 && errno == EINTR) {
      goto epoll_again;
    }
    for (int i = 0; i < rs; i++) {
      Event *e = (Event *) kernel_events[i].data.ptr;
      e->mask = kernel_events[i].events;
      sources[e->event_source_type]->OnEvent(e);
    }

    // fprintf(stderr, "epoll_awake\n");
    mutex.lock();
    waiting = false;

    goto again;
  }

  next_ctx = next->ctx;
  prev_ctx = stack_reuse ? nullptr : old_ctx;
  current = next;
  // l.unlock();

  if (stack_reuse) {
    // puts("Fast switch");
    setmcontext_light(&next_ctx->uc_mcontext);
  } else if (old_ctx != next_ctx) {
    // fprintf(stderr, "%d (%p) context switch (%p)%p=>(%p)%p, old stack %p new stack %p\n",
    //  	    tls_thread_pool_id, this,
    //  	    old, old_ctx, next, next_ctx, old_ctx->uc_stack.ss_sp, next_ctx->uc_stack.ss_sp);
    // always_assert(next->sched == this);

#if __has_feature(address_sanitizer)
    puts("start fiber switch");
    __sanitizer_start_switch_fiber(&old_ctx->asan_fake_stack,
                                   next_ctx->uc_stack.ss_sp,
                                   next_ctx->uc_stack.ss_size);
#endif
    swapcontext(old_ctx, next_ctx);
#if __has_feature(address_sanitizer)
    if (prev_ctx) {
      __sanitizer_finish_switch_fiber(old_ctx->asan_fake_stack,
                                      (const void **) &prev_ctx->uc_stack.ss_sp,
                                      &prev_ctx->uc_stack.ss_size);
    } else {
      void *fake_stack;
      size_t fake_stack_size;
      puts("finishing fiber switch");
      __sanitizer_finish_switch_fiber(old_ctx->asan_fake_stack,
                                      (const void **) &fake_stack, &fake_stack_size);
    }
#endif
  }
  Scheduler::Current()->mutex.unlock();
}

void Scheduler::WakeUp(Routine *r, bool batch)
{
  r->sched = this;
  ((ScheduleEventSource *) sources[ScheduleEventSourceType])->SendEvents(r, !batch);
}

void Scheduler::CollectGarbage()
{
  if (delay_garbage_ctx) {
    // fprintf(stderr, "Collecting %p stack %p\n", delay_garbage_ctx, delay_garbage_ctx->uc_stack.ss_sp);
    free(delay_garbage_ctx->uc_stack.ss_sp);
    free(delay_garbage_ctx);
    delay_garbage_ctx = nullptr;
  }
}

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

  if (g_thread_pool_should_exit.load())
    goto run_idle;

  ent = share_q.next;
  if (ent == &share_q)
    goto run_idle;

  r = (Routine *) ent;
  r->Detach();
  r->set_scheduler(sched);
  r->AddToReadyQueue(sched_ready_queue());
  return true;

run_idle:
  if (sched_current() != sched_idle_routine() || g_thread_pool_should_exit.load()) {
    sched_idle_routine()->AddToReadyQueue(sched_ready_queue());
    return true;
  } else {
    return false;
  }
}

void ScheduleEventSource::SendEvents(Routine *r, bool notify)
{
  auto old_sched = r->scheduler();
  LockScheduler(old_sched);
  r->Detach();
  r->set_scheduler(sched);

  if (r->is_share()) {
    std::lock_guard<std::mutex> _(share_m);
    r->AddToReadyQueue(&share_q);

    if (notify) {
      uint64_t u = 1;
      if (write(share_fd, &u, sizeof(uint64_t)) < sizeof(uint64_t)) {
        perror("write to share event fd");
        std::abort();
      }
    }
  } else {
    r->AddToReadyQueue(sched_ready_queue());

    if (notify && sched_is_waiting()) {
      uint64_t u = 1;
      if (write(fd, &u, sizeof(uint64_t)) < sizeof(uint64_t)) {
        perror("write to event fd");
        std::abort();
      }
    }
  }
  UnlockScheduler(old_sched);
}

int ScheduleEventSource::share_fd;
Scheduler::Queue ScheduleEventSource::share_q;
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
  ctx = (ucontext_t *) calloc(1, sizeof(ucontext_t)); // just a dummy context, no make context
}

void IdleRoutine::Run()
{
  while (!g_thread_pool_should_exit.load()) {
    Scheduler::Current()->RunNext(Scheduler::SleepState);
  }
}

void InitThreadPool(int nr_threads)
{
  ScheduleEventSource::Initialize();
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

	  auto idle_routine = new IdleRoutine;
	  idle_routine->Detach();

          auto sched = new Scheduler(idle_routine);
          tls_thread_pool_id = i;
          g_schedulers[tls_thread_pool_id] = sched;

	  nr_up.fetch_add(1);

          sched->StartRoutineStub();
	  idle_routine->Run();
	}));
  }
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
