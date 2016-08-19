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
  uint8_t *prealloc_data;
  size_t prealloc_len;
  uint8_t *prealloc_head;

  std::list<uint8_t *> buffer_pages;
  int nr_pages;
  int offset;
  int remain;
  bool eof;
  bool again;

  static const ssize_t kBufferPageSize = 16 << 10;
public:
  IOBuffer(size_t total_buffer_size = 0);

  void PreAllocBuffers(size_t total_buffer_size);
  uint8_t *AllocBuffer();
  void FreeBuffer(uint8_t *buf);

  void PushBack(const uint8_t *p, size_t sz);
  void PopFront(uint8_t *p, size_t sz);

  void Read(int fd, size_t max_size); // read to the end of the buffer
  void Write(int fd, size_t max_size); // write from the begin of the buffer

  bool is_empty() const {
    return size() == 0;
  }
  size_t size() const {
    return nr_pages * kBufferPageSize - offset - remain;
  }
  bool is_eof() const {
    return eof;
  }
  void set_eof() { eof = true; }
  void set_again() { again = true; }
  void clear_again() { again = false; }
  bool is_again() const { return again; }
};

class InputSocketChannelBase;
class OutputSocketChannelBase;
class AcceptSocketChannelBase;

class EpollSocketBase;

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
  OutType *output_channel() const { return (OutType *) out_channel; }
  AcceptType *accept_channel() const { return (AcceptType *) acc_channel; }
};

class InputSocketChannelBase {
protected:
  OptionalMutex mutex;
  IOBuffer q;
  size_t limit;
  EpollSocketBase *sock;
friend EpollThread;
friend EpollSocketBase;
  WaitSlot read_cv;
  bool single_thread;
public:
  InputSocketChannelBase(size_t lmt) : limit(lmt), q(lmt) {}
  void More(int amount);
  void NotifyMoreIO() {
    // fprintf(stderr, "notify more io\n");
    std::unique_lock<OptionalMutex> _(mutex);
    read_cv.Notify(read_cv.capacity());
  }
  OptionalMutex &buffer_mutex() { return mutex; }
};

template <class BaseClass = DummyChannel>
class InputSocketChannelImpl : public InputSocketChannelBase, public BaseClass {
public:
  using InputSocketChannelBase::InputSocketChannelBase;

  bool AcquireReadSpace(size_t size) {
    mutex.lock();
    while (q.size() < size) {
      More(size);
      if (q.size() >= size) break;
      if (q.is_eof()) return false;
      read_cv.WaitForSize(size, mutex.mutex_ptr());
    }
    return true;
  }
  void EndRead(size_t size) {
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
  void More(int amount);
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
      read_cv.WaitForSize(size * sizeof(int), mutex.mutex_ptr());
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
  OptionalMutex mutex;
  WaitSlot write_cv;
  IOBuffer q;
  size_t limit;
  EpollSocketBase *sock;
friend EpollThread;
friend EpollSocketBase;
  WaitSlot handler_cv;
public:
  OutputSocketChannelBase(size_t lmt) : limit(lmt) {}

  void AcquireWriteSpace(size_t size);
  void EndWrite(size_t size);

  void More(int amount);
  void NotifyMoreIO() {
    std::lock_guard<OptionalMutex> _(mutex);
    write_cv.Notify(write_cv.capacity());
  }

  OptionalMutex &buffer_mutex() { return mutex; }
};

template <class BaseClass = DummyChannel>
class OutputSocketChannelImpl : public OutputSocketChannelBase, public BaseClass {
public:
  using OutputSocketChannelBase::OutputSocketChannelBase;

  void AcquireWriteSpace(size_t size) {
    mutex.lock();
    if (limit > 0) {
      while (limit - q.size() < size && !q.is_eof()) {
	More(size);
	if (limit - q.size() >= size) break;
	if (q.is_eof()) break;
	write_cv.WaitForSize(size, mutex.mutex_ptr());
      }
    }
  }
  void EndWrite(size_t size) {
    if (limit == 0 && !q.is_eof()) {
      write_cv.WaitForSize(size, mutex.mutex_ptr());
    }
    sock->WatchWrite();
    mutex.unlock();
  }
  void WriteOne(const uint8_t &b) {
    q.PushBack(&b, 1);
  }
  void WriteAll(const uint8_t *buf, size_t cnt) {
    q.PushBack(buf, cnt);
  }
  void Close() {
    std::lock_guard<OptionalMutex> _(mutex);
    q.set_eof();
  }
  void Flush() {
    if (limit > 0) {
      std::lock_guard<OptionalMutex> _(mutex);
      while (!q.is_empty() && !q.is_eof()) {
	More(q.size());
	if (q.is_empty() || q.is_eof()) break;
	write_cv.WaitForSize(0, mutex.mutex_ptr());
      }
    }
  }
};

typedef OutputChannelWrapper<uint8_t, OutputSocketChannelImpl<DummyChannel> > OutputSocketChannel;

using EpollSocket = GenericEpollSocket<InputSocketChannel, AcceptSocketChannel, OutputSocketChannel>;

void CreateGlobalEpoll();
EpollThread *GlobalEpoll();

}

#endif /* EPOLL_CHANNEL_H */
