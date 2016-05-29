// -*- c++ -*-
#ifndef EPOLL_CHANNEL_H
#define EPOLL_CHANNEL_H

#include <sys/epoll.h>
#include <fcntl.h>
#include <list>
#include <mutex>
#include "gopp.h"

namespace go {

// zero-copy, dynamically increment buffer
class IOBuffer {
  std::list<uint8_t *> buffer_pages;
  int offset;
  int remain;
  bool eof;

  static const ssize_t kBufferPageSize = 4096;
public:
  IOBuffer();

  void PushBack(const uint8_t *p, size_t sz);
  void PopFront(uint8_t *p, size_t sz);

  void Read(int fd, size_t max_size); // read to the end of the buffer
  void Write(int fd, size_t max_size); // write from the begin of the buffer

  bool is_empty() const {
    return size() == 0;
  }
  size_t size() const {
    return buffer_pages.size() * kBufferPageSize - offset - remain;
  }
  bool is_eof() const {
    return eof;
  }
  void set_eof() { eof = true; }
};

class InputSocketChannelBase;
class OutputSocketChannelBase;
class AcceptSocketChannelBase;

struct EpollSocketBase;

class EpollThread {
  int fd;
  bool should_exit;
public:
  static const int kPollMaxEvents = 16;
  EpollThread();
  ~EpollThread();

  void ModifyWatchEvent(EpollSocketBase *data, uint32_t new_events);
  void EventLoop();

  void set_should_exit(bool b) { should_exit = b; }
};

class EpollSocketBase {
protected:
  InputSocketChannelBase *in_channel;
  OutputSocketChannelBase *out_channel;
  AcceptSocketChannelBase *acc_channel;
  EpollThread *poll;
  uint32_t events;
  std::mutex mutex;
  int fd;
friend EpollThread;
public:
  void WatchRead() {
    std::lock_guard<std::mutex> _(mutex);
    poll->ModifyWatchEvent(this, events | EPOLLIN);
  }
  void UnWatchRead() {
    std::lock_guard<std::mutex> _(mutex);
    poll->ModifyWatchEvent(this, events & ~EPOLLIN);
  }
  void WatchWrite() {
    std::lock_guard<std::mutex> _(mutex);
    poll->ModifyWatchEvent(this, events | EPOLLOUT);
  }
  void UnWatchWrite() {
    std::lock_guard<std::mutex> _(mutex);
    poll->ModifyWatchEvent(this, events & ~EPOLLOUT);
  }

  int file_desc() const { return fd; }

  enum SocketType {
    AcceptSocket,
    ReadSocket,
    WriteSocket,
    ReadWriteSocket,
  };
  EpollSocketBase(int file_desc, EpollThread *epoll, InputSocketChannelBase *in);
  EpollSocketBase(int file_desc, EpollThread *epoll, InputSocketChannelBase *in, OutputSocketChannelBase *out);
  EpollSocketBase(int file_desc, EpollThread *epoll, OutputSocketChannelBase *out);
  EpollSocketBase(int file_desc, EpollThread *epoll, AcceptSocketChannelBase *acc);

protected:
  EpollSocketBase(int file_desc, EpollThread *epoll);
private:
  SocketType type;
};

template <typename InType, typename AcceptType, typename OutType>
class GenericEpollSocket : public EpollSocketBase {
public:
  using EpollSocketBase::EpollSocketBase;

  InType *input_channel() const { return (InType *) in_channel; }
  AcceptType *output_channel() const { return (AcceptType *) out_channel; }
  OutType *accept_channel() const { return (OutType *) acc_channel; }
};

class InputSocketChannelBase {
protected:
  std::mutex mutex;
  SourceConditionVariable read_cv;
  IOBuffer q;
  size_t limit;
  EpollSocketBase *sock;
friend EpollThread;
friend EpollSocketBase;
public:
  InputSocketChannelBase(size_t lmt) : limit(lmt) {}
private:
  void HandleIO();
};

template <class BaseClass = DummyChannel>
class InputSocketChannelImpl : public InputSocketChannelBase, public BaseClass {
public:
  using InputSocketChannelBase::InputSocketChannelBase;

  bool AcquireReadSpace(size_t size) {
    mutex.lock();
  again:
    if (q.is_eof())
      return false;
    if (q.size() < size) {
      sock->WatchRead();
      read_cv.WaitForSize(size, &mutex);
      goto again;
    }
    return true;
  }
  void EndRead(size_t size) {
    if (read_cv.capacity() == 0) {
      sock->UnWatchRead();
    }
    mutex.unlock();
  }
  uint8_t ReadOne () {
    uint8_t b = 0;
    q.PopFront(&b, 1);
    return b;
  }
  void ReadAll(uint8_t *buf, size_t cnt) {
    q.PopFront(buf, cnt);
  }
};

typedef InputChannelWrapper<uint8_t, InputSocketChannelImpl<DummyChannel> > InputSocketChannel;

class AcceptSocketChannelBase : public InputSocketChannelBase {
friend EpollThread;
public:
  AcceptSocketChannelBase(size_t lmt) : InputSocketChannelBase(sizeof(int) * lmt) {}
private:
  void HandleIO();
};

template <class BaseClass = DummyChannel>
class AcceptSocketChannelImpl : public AcceptSocketChannelBase, public BaseClass {
friend EpollThread;
friend EpollSocketBase;
public:
  using AcceptSocketChannelBase::AcceptSocketChannelBase;

  bool AcquireReadSpace(size_t size) {
    mutex.lock();
    while (q.size() < size * sizeof(int)) {
      if (q.is_eof()) return false;
      sock->WatchRead();
      read_cv.WaitForSize(size * sizeof(int), &mutex);
    }
    return true;
  }
  void EndRead(size_t size) {
    if (read_cv.capacity() == 0) {
      sock->UnWatchRead();
    }
    mutex.unlock();
  }
  int ReadOne() {
    int new_fd = 0;
    q.PopFront((uint8_t *) &new_fd, sizeof(int));
    return new_fd;
  }

  void ReadAll(int *buf, size_t cnt) {
    q.PopFront((uint8_t *) buf, cnt * sizeof(int));
  }
};

typedef InputChannelWrapper<int, AcceptSocketChannelImpl<DummyChannel> > AcceptSocketChannel;

class OutputSocketChannelBase {
protected:
  std::mutex mutex;
  SourceConditionVariable write_cv;
  IOBuffer q;
  size_t limit;
  EpollSocketBase *sock;
friend EpollThread;
friend EpollSocketBase;
public:
  OutputSocketChannelBase(size_t lmt) : limit(lmt) {}

  void AcquireWriteSpace(size_t size);
  void EndWrite(size_t size);
private:
  void HandleIO();
};

template <class BaseClass = DummyChannel>
class OutputSocketChannelImpl : public OutputSocketChannelBase, public BaseClass {
public:
  using OutputSocketChannelBase::OutputSocketChannelBase;

  void AcquireWriteSpace(size_t size) {
    mutex.lock();
    if (limit > 0) {
      while (limit - q.size() < size && !q.is_eof()) {
	write_cv.WaitForSize(size, &mutex);
      }
    }
  }
  void EndWrite(size_t size) {
    if (limit == 0 && !q.is_eof()) {
      write_cv.WaitForSize(size, &mutex);
    }
    mutex.unlock();
  }
  void WriteOne(uint8_t b) {
    q.PushBack(&b, 1);
  }
  void WriteAll(uint8_t *buf, size_t cnt) {
    q.PushBack(buf, cnt);
  }
  void Close() {
    std::lock_guard<std::mutex> _(mutex);
    q.set_eof();
  }
  void Flush() {
    if (limit > 0) {
      std::lock_guard<std::mutex> _(mutex);
      while (!q.is_empty())
	write_cv.WaitForSize(0, &mutex);
    }
  }
};

typedef OutputChannelWrapper<int, OutputSocketChannelImpl<DummyChannel> > OutputSocketChannel;

using EpollSocket = GenericEpollSocket<InputSocketChannel, AcceptSocketChannel, OutputSocketChannel>;

}

#endif /* EPOLL_CHANNEL_H */
