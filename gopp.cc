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

#include <sys/time.h>

namespace go {

void Routine::Run0()
{
  sched->BottomHalf(this);
  Run();
  sched->RunNext(Scheduler::ExitState);
}

static void routine_func(void *r)
{
  ((Routine *) r)->Run0();
}

void Routine::InitStack(ucontext_t *link, size_t stack_size)
{
  ctx = (ucontext_t *) calloc(1, sizeof(ucontext_t));
  ctx->uc_stack.ss_sp = malloc(stack_size);
  ctx->uc_stack.ss_size = stack_size;
  ctx->uc_stack.ss_flags = 0;
  ctx->uc_link = link;

  makecontext(ctx, routine_func, this);
}

void Routine::InitFromGarbageContext(ucontext_t *c, void *sp)
{
  ctx = c;
  makecontext(ctx, routine_func, this);
}

static __thread int tls_thread_pool_id = -1;
static std::vector<Scheduler *> g_schedulers;
static bool g_thread_pool_should_exit = false;

Routine::Routine()
  : reuse(false), urgent(false), share(false), stack_ver(0)
{
  Reset();
}

void Routine::Reset()
{
  Init();
  sched = nullptr;
  ctx = nullptr;
}

void Routine::WakeUpOn(int tpid)
{
  if (tpid > g_schedulers.size() || tpid < 0) {
    fprintf(stderr, "Invalid thread id %d, not in the go-routine thread pool\n", tpid);
    return;
  }

  if (tpid > 0 && !g_schedulers[tpid]) {
    fprintf(stderr, "Thread id %d has not initialized\n", tpid);
    std::abort();
  }

  if (share && sched && sched->stack_ver != stack_ver) {
    // always_assert(sched->stack_ver > stack_ver);
    std::unique_lock<std::mutex> gl(Scheduler::share_m);
    Detach();
    Add(&Scheduler::share_q);
    gl.unlock();

    for (int i = 1; i < g_schedulers.size(); i++) {
      // fprintf(stderr, "notify share\n");
      std::lock_guard<std::mutex> _(g_schedulers[i]->mutex);
      g_schedulers[i]->cond.notify_one();
    }
    return;
  }

  auto target = sched;
  if (!target) {
    target = g_schedulers[tpid];
  }
  target->WakeUp(this);
}

void Routine::VoluntarilyPreempt(bool force)
{
  if (urgent)
    sched->RunNext(go::Scheduler::NextReadyState);
  else
    sched->RunNext(go::Scheduler::ReadyState);
}

Scheduler::Scheduler()
  : current(nullptr), delay_garbage_ctx(nullptr), stack_ver(0)
{
  ready_q.Init();
}

Scheduler::~Scheduler()
{
  CollectGarbage();
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

void Scheduler::RegisterScheduler(int tpid)
{
  tls_thread_pool_id = tpid;
  g_schedulers[tls_thread_pool_id] = new Scheduler();
}

void Scheduler::UnRegisterScheduler()
{
  delete g_schedulers[tls_thread_pool_id];
  g_schedulers[tls_thread_pool_id] = nullptr;
}

Scheduler *GetSchedulerFromPool(int thread_id)
{
  return g_schedulers[thread_id];
}

void Scheduler::Init(Routine *r)
{
  idle = current = r;
  idle->sched = this;
}

void Scheduler::RunNext(State state, Queue *sleep_q, std::mutex *sleep_lock)
{
  // fprintf(stderr, "[go] RunNext() on thread %d\n", tls_thread_pool_id);
  std::unique_lock<std::mutex> l(mutex);
  bool stack_reuse = false;
  bool should_delete_old = false;

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
    // fprintf(stderr, "routine %p (ctx %p) is garbage now\n", delay_garbage, delay_garbage_ctx);
  }
  old_ctx = old->ctx;

  if (should_delete_old) delete old;

again:
  auto ent = ready_q.next;

  if (ent != &ready_q) {
    next = (Routine *) ent;
    if (!next->ctx) {
      if (delay_garbage_ctx) {
	// reuse the stack and context memory
	// fprintf(stderr, "reuse ctx %p stack %p\n", delay_garbage_ctx, delay_garbage_ctx->uc_stack.ss_sp);
	next->InitFromGarbageContext(delay_garbage_ctx, delay_garbage_ctx->uc_stack.ss_sp);
	delay_garbage_ctx = nullptr;
	stack_reuse = true;
      } else {
	next->InitStack(idle->ctx, Routine::kStackSize);
      }
    }
    next->Detach();
  } else {
    std::unique_lock<std::mutex> gl(share_m);
    ent = share_q.next;
    if (ent != &share_q) {
      next = (Routine *) ent;
      next->Detach();
      next->sched = this;
      goto done;
    }

    if (g_thread_pool_should_exit) {
      next = idle;
      goto done;
    }

    gl.unlock();
    // timeval s; gettimeofday(&s, NULL);
    cond.wait(l);
    // timeval e; gettimeofday(&e, NULL);
    // fprintf(stderr, "%d waited for %lu ms\n", tls_thread_pool_id,
    // (e.tv_sec - s.tv_sec) * 1000 + (e.tv_usec - s.tv_usec) / 1000);

    goto again;
  }
done:
  next_ctx = next->ctx;
  current = next;
  l.unlock();

  if (stack_reuse) {
    setmcontext_light(&next_ctx->uc_mcontext);
  } else if (old_ctx != next_ctx) {
    // fprintf(stderr, "%d (%p) context switch (%p)%p=>(%p)%p, old stack %p new stack %p\n",
    //  	    tls_thread_pool_id, this,
    //  	    old, old_ctx, next, next_ctx, old_ctx->uc_stack.ss_sp, next_ctx->uc_stack.ss_sp);
    // always_assert(next->sched == this);
    swapcontext(old_ctx, next_ctx);
  } else {
    // always_assert(old == next);
  }
  old->sched->BottomHalf(old);
}

void Scheduler::BottomHalf(Routine *r)
{
  // always_assert(current == r);
  r->stack_ver = ++stack_ver;
}

void Scheduler::WakeUp(Routine *r, bool batch)
{
  std::lock_guard<std::mutex> _(mutex);
  r->Detach();
  if (r->urgent)
    r->Add(&ready_q);
  else
    r->Add(ready_q.prev);

  if (r->sched && r->sched != this) {
    std::abort();
    std::lock_guard<std::mutex> _(r->sched->mutex);
    r->sched->cond.notify_one();
  }
  r->sched = this;
  if (!batch) {
    cond.notify_one();
  }
}

void Scheduler::CollectGarbage()
{
  if (delay_garbage_ctx) {
    free(delay_garbage_ctx->uc_stack.ss_sp);
    free(delay_garbage_ctx);
    delay_garbage_ctx = nullptr;
  }
}

Scheduler::Queue Scheduler::share_q;
std::mutex Scheduler::share_m;

void Scheduler::InitShareQueue()
{
  share_q.Init();
}

static std::vector<std::thread> g_thread_pool;

// create a "System Idle Process" for scheduler

class IdleRoutine : public Routine {
public:
  IdleRoutine();
  virtual void Run();
};

IdleRoutine::IdleRoutine()
{
  ctx = (ucontext_t *) calloc(1, sizeof(ucontext_t)); // just a dummy context, no make context
}

void IdleRoutine::Run()
{
  while (!g_thread_pool_should_exit) {
    Scheduler::Current()->RunNext(Scheduler::SleepState);
  }
  Scheduler::Current()->CollectGarbage();
}

void InitThreadPool(int nr_threads)
{
  std::atomic<int> nr_up(0);
  g_schedulers.resize(nr_threads + 1, nullptr);
  Scheduler::InitShareQueue();

  for (int i = 0; i <= nr_threads; i++) {
    g_thread_pool.emplace_back(std::thread([&nr_up, i, nr_threads] {
	  // linux only
	  cpu_set_t set;
	  CPU_ZERO(&set);
	  if (i > 0) {
	    CPU_SET((i - 1) % sysconf(_SC_NPROCESSORS_CONF), &set);
	  } else {
	    for (int j = 0; j < nr_threads; j++) CPU_SET(j % sysconf(_SC_NPROCESSORS_CONF), &set);
	  }
	  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &set);

	  Scheduler::RegisterScheduler(i);
	  nr_up.fetch_add(1);

	  IdleRoutine idle_routine;
	  idle_routine.Detach();
	  Scheduler::Current()->Init(&idle_routine);
	  idle_routine.Run();
	}));
  }
  while (nr_up.load() <= nr_threads);
}

void RunOnMainThread()
{
  g_schedulers.push_back(nullptr); // WTF?
  g_thread_pool_should_exit = true;
  Scheduler::RegisterScheduler(1);
  Scheduler::Current()->RunNext(Scheduler::ExitState);
  Scheduler::Current()->CollectGarbage();
}

void WaitThreadPool()
{
  g_thread_pool_should_exit = true;
  for (auto sched: g_schedulers) {
    std::lock_guard<std::mutex> _(sched->mutex);
    sched->cond.notify_one();
  }

  for (auto &t: g_thread_pool) {
    t.join();
  }
}

void WaitSlot::WaitForSize(size_t size, std::mutex *lock)
{
  auto sched = Scheduler::Current();
  cap += size;
  sched->current_routine()->set_wait_for_delta(size);
  // fprintf(stderr, "%s %p sched %p\n", __FUNCTION__, sched->current_routine(), sched);
  sched->RunNext(Scheduler::SleepState, &sleep_q, lock);
  cap -= size;
  if (lock)
    lock->lock();
}

void WaitSlot::Notify(size_t new_cap)
{
  auto ent = sleep_q.next;
  while (ent != &sleep_q) {
    auto next = ent->next;
    auto r = (Routine *) ent;
    auto amt = r->wait_for_delta();
    // fprintf(stderr, "trying to wake up, amt %lu new_cap %lu\n", amt, new_cap);
    if (amt > new_cap) return;
    // fprintf(stderr, "%s %p\n", __FUNCTION__, r);
    r->WakeUp();

    new_cap -= amt;
    ent = next;
  }
}

}
