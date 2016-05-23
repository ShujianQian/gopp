#include <cstdio>

#include "gopp.h"

int main(int argc, char *argv[])
{
  go::InitThreadPool(2);

  auto ch = new go::BufferChannel<int>(0);

  auto leader = go::Make([ch]() {
      fprintf(stderr, "leader\n");
      for (int i = 0; i < 10; i++) {
	fprintf(stderr, "push %d\n", i);
	ch->Write(i);
      }
    });

  auto follower = go::Make([ch]() {
      fprintf(stderr, "follower\n");
      for (int i = 0; i < 10; i++) {
	fprintf(stderr, "Follower got %d\n", ch->Read());
      }
    });

  leader->StartOn(1);
  follower->StartOn(2);

  go::WaitThreadPool();
  return 0;
}
