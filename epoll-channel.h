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

struct EPollSocket;

class EPollThread {
  int fd;
  bool should_exit;
public:
  static const int kPollMaxEvents = 16;
  EPollThread();
  ~EPollThread();

  void ModifyWatchEvent(EPollSocket *data, uint32_t new_events);
  void EventLoop();

  void set_should_exit(bool b) { should_exit = b; }
};

class EPollSocket {
  InputSocketChannelBase *in_channel;
  OutputSocketChannelBase *out_channel;
  AcceptSocketChannelBase *acc_channel;
  EPollThread *poll;
  uint32_t events;
  std::mutex mutex;
  int fd;
friend EPollThread;
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
  InputSocketChannelBase *input_channel() const { return in_channel; }
  OutputSocketChannelBase *output_channel() const { return out_channel; }
  AcceptSocketChannelBase *accept_channel() const { return acc_channel; }

  enum SocketType {
    AcceptSocket,
    ReadSocket,
    WriteSocket,
    ReadWriteSocket,
  };
  EPollSocket(int file_desc, EPollThread *epoll, InputSocketChannelBase *in);
  EPollSocket(int file_desc, EPollThread *epoll, InputSocketChannelBase *in, OutputSocketChannelBase *out);
  EPollSocket(int file_desc, EPollThread *epoll, OutputSocketChannelBase *out);
  EPollSocket(int file_desc, EPollThread *epoll, AcceptSocketChannelBase *acc);

protected:
  EPollSocket(int file_desc, EPollThread *epoll);
private:
  SocketType type;
};

class InputSocketChannelBase {
protected:
  std::mutex mutex;
  SourceConditionVariable read_cv;
  IOBuffer q;
  size_t limit;
  EPollSocket *sock;
friend EPollThread;
friend EPollSocket;
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
    while (q.size() < size) {
      if (q.is_eof()) return false;
      sock->WatchRead();
      read_cv.WaitForSize(size, &mutex);
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
friend EPollThread;
public:
  AcceptSocketChannelBase(size_t lmt) : InputSocketChannelBase(sizeof(int) * lmt) {}
private:
  void HandleIO();
};

template <class BaseClass = DummyChannel>
class AcceptSocketChannelImpl : public AcceptSocketChannelBase, public BaseClass {
friend EPollThread;
friend EPollSocket;
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
  int ReadOne(bool &eof) {
    int new_fd = 0;
    if (q.is_empty() && q.is_eof()) {
      eof = true;
    } else {
      eof = false;
      q.PopFront((uint8_t *) &new_fd, sizeof(int));
    }
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
  EPollSocket *sock;
friend EPollThread;
friend EPollSocket;
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

}

#endif /* EPOLL_CHANNEL_H */
