#include <cstdio>
#include <cstring>
#include <unistd.h>

#include <sstream>
#include <thread>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "gopp.h"
#include "epoll-channel.h"

int main(int argc, char *argv[])
{
  go::InitThreadPool();
  go::CreateGlobalEpoll();

  std::stringstream ss;

  int fd = socket(AF_INET, SOCK_STREAM, 0);

  struct sockaddr_in sock_addr;
  memset(&sock_addr, 0, sizeof(struct sockaddr_in));
  sock_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
  sock_addr.sin_family = AF_INET;
  sock_addr.sin_port = htons(8000);

  if (connect(fd, (struct sockaddr *) &sock_addr,
	      sizeof(struct sockaddr_in)) < 0) {
    perror("connect");
    std::abort();
  }
  std::mutex finish_lock;
  finish_lock.lock();

  auto r = go::Make([fd, &finish_lock] {
      auto sock = new go::EpollSocket(fd, go::GlobalEpoll(),
				      new go::InputSocketChannel(4096),
				      new go::OutputSocketChannel(4096));
      auto out = sock->output_channel();
      std::stringstream ss;
      ss << "GET / HTTP/1.0\r\n\r\n";
      out->Write(ss.str().c_str(), ss.str().length());
      out->Flush();
      puts("Request sent");
      uint8_t ch = 0;
      while (sock->input_channel()->Read(&ch)) {
	putchar(ch);
      }
      finish_lock.unlock();
    });
  r->StartOn(1);

  auto t = std::thread([]{
      go::GlobalEpoll()->EventLoop();
    });
  t.detach();

  finish_lock.lock();
  go::WaitThreadPool();
  return 0;
}
