#include "epoll-channel.h"
#include <cstdio>
#include <cassert>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <sys/socket.h>

namespace go {

IOBuffer::IOBuffer()
{
  offset = 0;
  remain = kBufferPageSize;
  buffer_pages.push_back((uint8_t *) malloc(kBufferPageSize));
}

void IOBuffer::PushBack(const uint8_t *p, size_t sz)
{
  ssize_t left = sz;

  while (left > 0) {
    if (remain == 0) {
      buffer_pages.push_back((uint8_t *) malloc(kBufferPageSize));
      remain = kBufferPageSize;
    }
    size_t to_copy = left < remain ? left : remain;
    memcpy(buffer_pages.back() + kBufferPageSize - remain, p, to_copy);
    p += to_copy;
    remain -= to_copy;
    left -= to_copy;
  }
}

void IOBuffer::PopFront(uint8_t *p, size_t sz)
{
  size_t left = sz;
  while (left > 0) {
    auto page_ptr = buffer_pages.front();

    size_t to_copy = kBufferPageSize - offset;
    if (page_ptr == buffer_pages.back()) to_copy -= remain;
    if (left < to_copy) to_copy = left;

    memcpy(p, page_ptr + offset, to_copy);

    p += to_copy;
    offset += to_copy;
    left -= to_copy;

    if (offset == kBufferPageSize) {
      offset = 0;
      free(buffer_pages.front());
      buffer_pages.pop_front();
    }
  }
}

void IOBuffer::Read(int fd, size_t max_len)
{
  int maxiovcnt = (max_len - 1) / kBufferPageSize + 2;
  struct iovec iov[maxiovcnt];
  size_t left = max_len;
  int iovcnt = 0;

  while (left > 0) {
    size_t to_copy = 0;
    uint8_t *buf = nullptr;

    if (iovcnt == 0) {
      to_copy = remain;
      buf = buffer_pages.back() + kBufferPageSize - remain;
    } else {
      to_copy = kBufferPageSize;
      buf = (uint8_t *) malloc(kBufferPageSize);
    }

    if (left < to_copy) to_copy = left;

    iov[iovcnt].iov_base = buf;
    iov[iovcnt].iov_len = to_copy;
    left -= to_copy;
    iovcnt++;
  }
  auto rs = readv(fd, iov, iovcnt);
  if (rs < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
    rs = 0;
  } else if (rs == 0) {
    eof = true;
  }

  if (rs < 0) {
    perror("reav");
    return;
  }

  left = 0;
  for (int i = 0; i < iovcnt; i++) {
    left += iov[i].iov_len;
    if (i > 0)
      buffer_pages.push_back((uint8_t *) iov[i].iov_base);

    if (left >= rs) {
      remain = (remain - rs) % kBufferPageSize + kBufferPageSize;
      remain %= kBufferPageSize;
      for (int j = i + 1; j < iovcnt; j++) {
	free(iov[j].iov_base);
      }
      break;
    }
  }
}

void IOBuffer::Write(int fd, size_t max_len)
{
  int maxiovcnt = (max_len - 1) / kBufferPageSize + 2;
  struct iovec iov[maxiovcnt];
  int iovcnt = 0;
  size_t left = max_len;
  auto it = buffer_pages.begin();

  while (left > 0) {
    size_t to_copy = 0;
    uint8_t *buf = nullptr;

    if (iovcnt == 0) {
      to_copy = kBufferPageSize - offset;
      buf = (*it) + offset;
    } else {
      to_copy = kBufferPageSize;
      buf = (*it);
    }

    if (left < to_copy) to_copy = left;

    iov[iovcnt].iov_base = buf;
    iov[iovcnt].iov_len = to_copy;
    left -= to_copy;
    iovcnt++;
    ++it;
  }

  auto rs = writev(fd, iov, iovcnt);

  if (rs < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      rs = 0;
    } else if (errno == EPIPE) {
      eof = true;
      rs = 0;
    } else {
      perror("writev");
      return;
    }
  }

  left = 0;
  for (int i = 0; i < iovcnt; i++) {
    left += iov[i].iov_len;
    if (left >= rs) {
      offset = (offset + rs + kBufferPageSize - 1) % kBufferPageSize + 1;
      break;
    } else {
      buffer_pages.pop_front();
    }
  }
}

EPollThread::EPollThread()
{
  fd = epoll_create(1);
}

EPollThread::~EPollThread()
{
  close(fd);
}

void EPollThread::ModifyWatchEvent(EPollSocket *data, uint32_t new_events)
{
  struct epoll_event event;
  event.events = new_events;
  event.data.ptr = data;
  if (new_events == 0 && data->events != 0) {
    if (epoll_ctl(fd, EPOLL_CTL_DEL, data->fd, &event) < 0) {
      goto fail;
    }
  } else if (data->events == 0 && new_events != 0) {
    if (epoll_ctl(fd, EPOLL_CTL_ADD, data->fd, &event) < 0) {
      goto fail;
    }
  } else if (data->events != new_events) {
    if (epoll_ctl(fd, EPOLL_CTL_MOD, data->fd, &event) < 0) {
      goto fail;
    }
  }
  data->events = new_events;
  return;
fail:
  perror("epoll_ctl");
  std::abort();
}

void EPollThread::EventLoop()
{
  while (!should_exit) {
    struct epoll_event evt[kPollMaxEvents];
    auto rs = epoll_wait(fd, evt, kPollMaxEvents, 1000);
    if (rs < 0) {
      if (errno == EINTR) continue;
      else {
	perror("epoll");
	std::abort();
      }
    }
    if (rs == 0) continue;
    for (int i = 0; i < rs; i++) {
      auto data = (EPollSocket *) evt[i].data.ptr;
      auto events = evt[i].events;
      if (events & EPOLLIN) {
	if (data->type == EPollSocket::AcceptSocket) {
	  data->acc_channel->HandleIO();
	} else {
	  data->in_channel->HandleIO();
	}
      }
      if (events & EPOLLOUT) {
	data->out_channel->HandleIO();
      }
    }
  }
}

EPollSocket::EPollSocket(int file_desc, EPollThread *epoll)
  : in_channel(nullptr), out_channel(nullptr), acc_channel(nullptr), poll(epoll), events(0),
    fd(file_desc)
{
  fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
}


EPollSocket::EPollSocket(int file_desc, EPollThread *epoll, InputSocketChannelBase *in)
  : EPollSocket(file_desc, epoll)
{
  type = ReadSocket;
  in_channel = in;
  in_channel->sock = this;
}

EPollSocket::EPollSocket(int file_desc, EPollThread *epoll, InputSocketChannelBase *in, OutputSocketChannelBase *out)
  : EPollSocket(file_desc, epoll)
{
  type = ReadWriteSocket;
  in_channel = in;
  in_channel->sock = this;
  out_channel = out;
  out_channel->sock = this;
}

EPollSocket::EPollSocket(int file_desc, EPollThread *epoll, OutputSocketChannelBase *out)
  : EPollSocket(file_desc, epoll)
{
  type = WriteSocket;
  out_channel = out;
  out_channel->sock = this;
}

EPollSocket::EPollSocket(int file_desc, EPollThread *epoll, AcceptSocketChannelBase *acc)
  : EPollSocket(file_desc, epoll)
{
  type = AcceptSocket;
  acc_channel = acc;
  acc_channel->sock = this;
}

void InputSocketChannelBase::HandleIO()
{
  std::lock_guard<std::mutex> _(mutex);
  q.Read(sock->file_desc(), limit == 0 ? read_cv.capacity() : limit - q.size());
  read_cv.Notify(q.size());
}

void AcceptSocketChannelBase::HandleIO()
{
  std::lock_guard<std::mutex> _(mutex);
  int nr = limit == 0 ? read_cv.capacity() / sizeof(int) : (limit - q.size()) / sizeof(int);
  for (int i = 0; i < nr; i++) {
  again:
    struct sockaddr addr;
    socklen_t len = 0;
    memset(&addr, 0, sizeof(struct sockaddr));
    int new_sock = accept(sock->file_desc(), &addr, &len);
    if (new_sock < 0) {
      if (errno == EWOULDBLOCK || errno == EAGAIN) {
	break;
      } else if (errno == EINTR) {
	goto again;
      }
      perror("accept");
      continue;
    }
    q.PushBack((uint8_t *) &new_sock, sizeof(int));
  }
  read_cv.Notify(q.size());
}

void OutputSocketChannelBase::HandleIO()
{
  std::lock_guard<std::mutex> _(mutex);
  q.Write(sock->file_desc(), q.size());
  if (limit == 0)
    write_cv.Notify(write_cv.capacity() - q.size());
  else
    write_cv.Notify(limit - q.size());
}

}

#if 0
// test
int main(int argc, char *argv[])
{
  go::IOBuffer buf;
  int *tmp = new int[2048];

  for (int i = 0; i < 10000; i++) {
    int repeat = (i ^ 2166136261) * 16777619 % 2048;
    for (int j = 0; j < repeat; j++) tmp[j] = i;
    buf.PushBack((uint8_t *) tmp, sizeof(int) * repeat);
  }
  size_t tot = buf.size();
  printf("%lu\n", buf.size());
  int fd = open("dump", O_CREAT | O_RDWR, 0644);
  while (!buf.is_empty()) {
    int wsize = rand() % 16192;
    if (wsize > buf.size()) wsize = buf.size();
    buf.Write(fd, wsize);
  }
  close(fd);
  assert(buf.size() == 0);

  fd = open("dump", O_RDWR);
  while (buf.size() < tot) {
    int rsize = rand() % 16192;
    if (rsize > tot - buf.size()) rsize = tot - buf.size();
    buf.Read(fd, rsize);
  }
  close(fd);
  assert(buf.size() == tot);
  for (int i = 0; i < 10000; i++) {
    int repeat = (i ^ 2166136261) * 16777619 % 2048;
    buf.PopFront((uint8_t *) tmp, sizeof(int) * repeat);
    for (int j = 0; j < repeat; j++)
      assert(tmp[j] == i);
  }
  assert(buf.size() == 0);
  return 0;
}

#endif
