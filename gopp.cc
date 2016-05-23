#include "gopp.h"
#include <cassert>
#include <cstring>
#include <vector>
#include <pthread.h>
#include <ucontext.h>
#include <cstdarg>
#include <atomic>
#include <thread>

namespace go {

static void routine_func(Routine *routine)
{
  routine->Run0();
}

void Routine::InitStack(ucontext_t *link, size_t stack_size)
{
  memset(&ctx, 0, sizeof(ucontext_t));
  ctx.uc_stack.ss_sp = malloc(stack_size);
  ctx.uc_stack.ss_size = stack_size;
  ctx.uc_link = link;
  ctx.uc_mcontext.fpregs = &ctx.__fpregs_mem; /* weird, why this is undocumented? */

  makecontext(&ctx, (void (*)()) &routine_func, 1, this);
  // fprintf(stderr, "Allocated stack %lu\n", stack_size);
}

static __thread int tls_thread_pool_id = 0;
static std::vector<Scheduler *> g_schedulers;
static bool g_thread_pool_should_exit = false;

Routine::Routine()
  : sched(nullptr)
{
  memset(&ctx, 0, sizeof(ucontext_t));
  Init();
}

void Routine::WakeUpOn(int tpid)
{
  if (tpid > g_schedulers.size() || tpid < 0) {
    fprintf(stderr, "Invalid thread id %d, not in the go-routine thread pool\n", tpid);
    return;
  }
  if (tpid == 0 && sched == nullptr) {
    fprintf(stderr, "This go-routine is a new one, cannot add to its current scheduler\n");
    return;
  }
  if (!g_schedulers[tpid - 1]) {
    fprintf(stderr, "Thread id %d has not initialized\n", tpid);
    std::abort();
  }
  auto target = sched;
  if (tpid > 0) {
    target = g_schedulers[tpid - 1];
  }
  target->WakeUp(this);
}

Scheduler::Scheduler()
  : resp_count(0), current(nullptr), delay_garbage(nullptr)
{
  ready_q.Init();
  memset(&host_ctx, 0, sizeof(ucontext_t));
}

Scheduler::~Scheduler()
{
  CollectGarbage();
}

Scheduler *Scheduler::Current()
{
  if (tls_thread_pool_id == 0) {
    fprintf(stderr, "Not a go-routine thread, return null on %s\n", __FUNCTION__);
    return nullptr;
  }
  auto res = g_schedulers[tls_thread_pool_id - 1];
  if (!res) {
    fprintf(stderr, "This go-routine thread has not associate with a scheduler\n");
  }
  return res;
}

void Scheduler::RegisterScheduler(int tpid)
{
  tls_thread_pool_id = tpid;
  g_schedulers[tls_thread_pool_id - 1] = new Scheduler();
}

void Scheduler::UnRegisterScheduler()
{
  delete g_schedulers[tls_thread_pool_id - 1];
  g_schedulers[tls_thread_pool_id - 1] = nullptr;
}

void Scheduler::RunNext(State state, Queue *sleep_q, std::mutex *sleep_lock)
{
  // fprintf(stderr, "RunNext() on thread %d\n", tls_thread_pool_id);
  std::unique_lock<std::mutex> l(mutex);

  CollectGarbage();
  auto old = current;
  ucontext_t *old_ctx = nullptr, *current_ctx = nullptr;

  if (old == nullptr) {
    old_ctx = &host_ctx;
  } else {
    if (state == SleepState) {
      old->Add(sleep_q->prev);
      sleep_lock->unlock();
    } else if (state == ReadyState) {
      old->Add(ready_q.prev);
    } else if (state == ExitState) {
      resp_count--;
      delay_garbage = old;
    }
    old_ctx = &old->ctx;
  }

  current = nullptr;

again:
  auto ent = ready_q.next;

  if (ent != &ready_q) {
    current = (Routine *) ent;
    if (!current->ctx.uc_stack.ss_sp)
      current->InitStack(&host_ctx, Routine::kStackSize);
    current->Detach();
    current_ctx = &current->ctx;
  } else if (g_thread_pool_should_exit && resp_count == 0) {
    current_ctx = &host_ctx;
  } else {
    // fprintf(stderr, "no routine, wait. thread id %d\n", tls_thread_pool_id);
    cond.wait(l);
    goto again;
  }

  l.unlock();
  if (current != old)
    swapcontext(old_ctx, current_ctx);
}

void Scheduler::WakeUp(Routine *r)
{
  std::unique_lock<std::mutex> l1, l2;
  if (r->sched != this && r->sched != nullptr) {
    std::lock(mutex, r->sched->mutex);
    l1 = std::unique_lock<std::mutex>(mutex, std::adopt_lock);
    l2 = std::unique_lock<std::mutex>(r->sched->mutex, std::adopt_lock);
  } else {
    l1 = std::unique_lock<std::mutex>(mutex);
  }
  if (r->sched != nullptr && r == r->sched->current)
    return;
  r->Detach();
  r->Add(ready_q.prev);
  if (r->sched) {
    r->sched->resp_count--;
    if (r->sched != this)
      r->sched->cond.notify_one();
  }
  r->sched = this;
  resp_count++;
  // fprintf(stderr, "%p scheduler notified\n", this);
  cond.notify_one();
}

void Scheduler::CollectGarbage()
{
  if (delay_garbage) {
    free(delay_garbage->ctx.uc_stack.ss_sp);
    delete delay_garbage;
  }
  delay_garbage = nullptr;
}

static std::vector<std::thread> g_thread_pool;

void InitThreadPool(int nr_threads)
{
  std::atomic<int> nr_up(0);
  g_schedulers.resize(nr_threads, nullptr);
  for (int i = 0; i < nr_threads; i++) {
    g_thread_pool.emplace_back(std::move(std::thread([&nr_up, i]{
	    Scheduler::RegisterScheduler(i + 1);
	    nr_up.fetch_add(1);
	    Scheduler::Current()->RunNext(Scheduler::ExitState);
	    Scheduler::Current()->CollectGarbage();
	  })));
  }
  while (nr_up.load() < nr_threads);
}

void WaitThreadPool()
{
  g_thread_pool_should_exit = true;
  for (auto sched: g_schedulers) {
    sched->Signal();
  }
  for (auto &t: g_thread_pool) {
    t.join();
  }
}

void SourceConditionVariable::WaitForSize(size_t size, std::mutex *lock)
{
  auto sched = Scheduler::Current();
  sched->current_routine()->set_wait_for_delta(size);
  sched->RunNext(Scheduler::SleepState, &sleep_q, lock);
  lock->lock();
}

void SourceConditionVariable::Notify(size_t delta)
{
  auto ent = sleep_q.next;
  while (ent != &sleep_q) {
    auto next = ent->next;
    auto r = (Routine *) ent;
    auto amt = r->wait_for_delta();
    // fprintf(stderr, "trying to wake up, amt %lu delta %lu\n", amt, delta);
    if (amt > delta) return;
    r->WakeUp();

    delta -= amt;
    ent = next;
  }
}

}
