#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unistd.h>
#include <sys/eventfd.h>
#include "gopp.h"
#include "channels.h"

class Init : public go::Routine {
 public:
  void Run() final {
    puts("Init");
  }
};

class Alice : public go::Routine {
 public:
  void Run() final;
};

class Bob : public go::Routine {
 public:
  void Run() final;
};

go::BufferChannel *chn = new go::BufferChannel(8192);
int donefd = eventfd(0, EFD_SEMAPHORE);

void Verify(char *s, int t, size_t sz)
{
  for (int i = 0; i < sz; i++)
    if (s[i] != (i * 59 + t * 117 + 31) % 128)
      std::abort();
}

static int kMaxTimes = 10000;

void Alice::Run()
{
  char s[8192];
  for (int t = 0; t < kMaxTimes; t++) {
    size_t sz = (t * 159 + 23) % 8192;
    for (int i = 0; i < sz; i++)
      s[i] = (i * 59 + t * 117 + 31) % 128;
    chn->Write(s, sz);
    Verify(s, t, sz);
    if (t % 100 == 0)
      fprintf(stderr, "%d/%d\r", t, kMaxTimes);
  }
  uint64_t u = 1;
  write(donefd, &u, sizeof(uint64_t));
}

void Bob::Run()
{
  char s[8192];
  for (int t = 0; t < kMaxTimes; t++) {
    size_t sz = (t * 159 + 23) % 8192;
    if (!chn->Read(s, sz)) {
      std::abort();
    }
    Verify(s, t, sz);
  }
  uint64_t u = 1;
  write(donefd, &u, sizeof(uint64_t));
}

int main(int argc, char *argv[])
{
  go::InitThreadPool(2);
  auto a = new Alice();
  auto b = new Bob();

  go::GetSchedulerFromPool(1)->WakeUp(new Init());
  go::GetSchedulerFromPool(1)->WakeUp(b);
  go::GetSchedulerFromPool(1)->WakeUp(a);

  uint64_t u;
  read(donefd, &u, sizeof(uint64_t));
  read(donefd, &u, sizeof(uint64_t));
  go::WaitThreadPool();

  return 0;
}
