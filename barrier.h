#ifndef BARRIER_H
#define BARRIER_H

#include <mutex>
#include "gopp.h"

namespace go {

class WaitBarrier {
  std::mutex m;
  go::Scheduler::Queue sleep_q;
  size_t cnt;
 public:
  WaitBarrier(size_t init_count = 1) : cnt(init_count) {
    sleep_q.Init();
  }
  void Wait() {
    m.lock();
    cnt--;
    if (cnt == 0) {
      auto *ent = sleep_q.next;
      while (ent != &sleep_q) {
        auto next = ent->next;
        auto r = (Routine *) ent;
        r->scheduler()->WakeUp(r);
        ent = next;
      }
      m.unlock();
    } else {
      go::Scheduler::Current()->RunNext(go::Scheduler::SleepState, &sleep_q, &m);
    }
  }
  void Adjust(size_t nr_wait) { cnt = nr_wait; }
};

}

#endif /* BARRIER_H */
