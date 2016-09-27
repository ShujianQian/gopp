#include <cstdio>
#include <netdb.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <fcntl.h>

#include "gopp.h"
#include "epoll-channel.h"

class Connection : public go::Routine {
  go::EpollSocket *sock;
public:
  Connection(go::EpollSocket *s) : sock(s) {};

  virtual void Run() {
    fprintf(stderr, "New Client\n");
    auto in = sock->input_channel();
    while (true) {
      uint8_t ch;
      if (!in->Read(&ch)) {
	fprintf(stderr, "client closed\n");
	close(sock->file_desc());
	return;
      }
      putchar(ch);
    }
    fprintf(stderr, "Done with this client\n");
  }
};

class ServerAcceptor : public go::Routine {
  int fd;
  go::EpollThread *poll;
public:
  ServerAcceptor(int file_desc, go::EpollThread *epoll) : fd(file_desc), poll(epoll) {}
  virtual void Run() {
    auto ch = new go::AcceptSocketChannel(10);
    new go::EpollSocketBase(fd, poll, ch);
    while (true) {
      bool eof = false;
      fprintf(stderr, "server reading fd\n");
      int client_fd = ch->Read(eof);

      if (eof) {
	fprintf(stderr, "server socket accidentially closed\n");
	std::abort();
      }
      fprintf(stderr, "got new client %d\n", client_fd);

      auto sock = new go::EpollSocket(client_fd, poll, new go::InputSocketChannel(10));
      auto client_routine = new Connection(sock);
      client_routine->StartOn(1);
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

  go::EpollThread *poll = new go::EpollThread();

  auto main_routine = new ServerAcceptor(fd, poll);
  main_routine->StartOn(1);

  poll->EventLoop();

  go::WaitThreadPool();
  return 0;
}