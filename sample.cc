#include <cstdio>
#include <netdb.h>
#include <netinet/in.h>
#include <cstring>
#include <fcntl.h>

#include "gopp.h"
#include "epoll-channel.h"

class Connection : public go::Routine {
  go::InputSocketChannel *in_channel;
  go::OutputSocketChannel *out_channel;
public:
  Connection(go::InputSocketChannel *in, go::OutputSocketChannel *out) :
    in_channel(in), out_channel(out) {}

  virtual void Run() {
    fprintf(stderr, "New Client\n");
  }
};

class ServerAcceptor : public go::Routine {
  int fd;
  go::EPollThread *poll;
public:
  ServerAcceptor(int file_desc, go::EPollThread *epoll) : fd(file_desc), poll(epoll) {}
  virtual void Run() {
    auto ch = new go::AcceptSocketChannel(10);
    new go::EPollSocket(fd, poll, ch);
    while (true) {
      bool eof = false;
      int client_fd = ch->Read(eof);
      fprintf(stderr, "got new client %d\n", client_fd);

      auto rch = new go::InputSocketChannel(10);
      new go::EPollSocket(fd, poll, rch);
      (new Connection(rch, nullptr))->StartOn(1);
    }
  }
};

int main(int argc, char *argv[])
{
  go::InitThreadPool(1);

  struct sockaddr_in addr;
  int fd = socket(AF_INET, SOCK_STREAM, 0);

  if (fd < 0) {
    perror("socket");
    exit(-1);
  }

  memset(&addr, 0, sizeof(struct sockaddr_in));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(1122);

  if (bind(fd, (struct sockaddr *) &addr, sizeof(struct sockaddr_in)) < 0) {
    perror("bind");
    exit(-1);
  }

  if (listen(fd, 100) < 0) {
    perror("listen");
    exit(-1);
  }

  fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);

  go::EPollThread *poll = new go::EPollThread();

  auto main_routine = new ServerAcceptor(fd, poll);
  main_routine->StartOn(1);

  poll->EventLoop();

  go::WaitThreadPool();
  return 0;
}
