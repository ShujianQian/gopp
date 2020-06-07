#include <sys/epoll.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstdlib>
#include <cstring>
#include "channels.h"

namespace go {

class IOBuffer {
  struct Page {
    struct Page *next, *prev;
    uint8_t *data;

    void Add(Page *parent) {
      next = parent->next;
      prev = parent;

      parent->next->prev = this;
      parent->next = this;
    }

    void Detach() {
      prev->next = next;
      next->prev = prev;

      next = prev = this;
    }
  };

  static size_t NrPages(size_t sz) {
    if (sz == 0) return 0;
    return (sz - 1) / kPageSize + 1;
  }
  static size_t AlignUp(size_t sz) {
    return kPageSize * NrPages(sz);
  }

  Page head;

  Page *freelist;
  Page *all_pages;
  uint8_t *all_data;

  int offset = 0;
  size_t size = 0;
  size_t nr_total_pages;

  void Grow(size_t delta, bool front);
  void Shrink(size_t delta, bool front);

 public:

  static const size_t kPageSize = 4096;

  IOBuffer(size_t capacity);
  ~IOBuffer();

  void GrowBack(size_t delta) { Grow(delta, false); }
  void GrowFront(size_t delta) { Grow(delta, true); }
  void ShrinkBack(size_t delta) { Shrink(delta, false); }
  void ShrinkFront(size_t delta) { Shrink(delta, true); }

  static const size_t kBatchIOSize = 256 << 10;

  template <typename Func>
  bool ReadFrom(Func callback);

  template <typename Func>
  void WriteTo(Func callback);

  size_t Peek(void *data, size_t cnt);
  void Pop(void *data, size_t cnt);
  void Push(const void *data, size_t cnt);

  size_t buffer_size() { return size; }
  size_t capacity() { return nr_total_pages * kPageSize; }
  size_t max_grow_back_space() {
    return nr_total_pages * kPageSize - offset - size;
  }
  size_t max_grow_front_space() {
    return nr_total_pages * kPageSize - AlignUp(offset + size) + offset;
  }
};

const size_t IOBuffer::kBatchIOSize;
const size_t IOBuffer::kPageSize;

IOBuffer::IOBuffer(size_t capacity)
    : nr_total_pages(NrPages(capacity))
{
  all_pages = new Page[nr_total_pages];
  all_data = new uint8_t[nr_total_pages * kPageSize];

  head.next = head.prev = &head;

  freelist = &all_pages[0];
  for (int i = 0; i < nr_total_pages - 1; i++) {
    all_pages[i].next = &all_pages[i + 1];
  }
  all_pages[nr_total_pages - 1].next = nullptr;

  for (int i = 0; i < nr_total_pages; i++) {
    all_pages[i].data = &all_data[i * kPageSize];
  }
}

IOBuffer::~IOBuffer()
{
  delete [] all_pages;
  delete [] all_data;
}

void IOBuffer::Grow(size_t delta, bool front)
{
  // fprintf(stderr, "Grow: offset %d size %d delta %d\n", offset, size, delta);
  int nr_pages = 0;
  if (front) {
    nr_pages = NrPages(delta + kPageSize - offset) - 1;
  } else {
    nr_pages = NrPages(offset + size + delta) - NrPages(offset + size);
  }
  Page *pg = nullptr;
  for (int i = 0; i < nr_pages; i++) {
    pg = freelist;
    freelist = freelist->next;
    if (front)
      pg->Add(&head);
    else
      pg->Add(head.prev);
  }
  size += delta;
  if (front) {
    offset = offset + kPageSize * nr_pages - delta;
  }
  // fprintf(stderr, "Grow: offset %d size %d\n", offset, size);
}

void IOBuffer::Shrink(size_t delta, bool front)
{
  // fprintf(stderr, "Shrink: offset %d size %d delta %d\n", offset, size, delta);
  int nr_pages = 0;
  if (front) {
    nr_pages = (offset + delta) / kPageSize;
  } else {
    nr_pages = NrPages(offset + size) - NrPages(offset + size - delta);
  }
  Page *pg = nullptr;
  for (int i = 0; i < nr_pages; i++) {
    if (front)
      pg = head.next;
    else
      pg = head.prev;

    pg->Detach();
    pg->next = freelist;
    freelist = pg;
  }
  size -= delta;
  if (front) {
    offset = offset + delta - kPageSize * nr_pages;
  }
  // fprintf(stderr, "Shrink: offset %d size %d\n", offset, size);
}

template <typename Func>
bool IOBuffer::ReadFrom(Func callback)
{
  size_t iosize = std::min(max_grow_back_space(), kBatchIOSize);
  struct iovec iov[kBatchIOSize / kPageSize + 1];
  int iovcnt = 0;
  size_t acc_iosize = 0;
  Page *last_page = head.prev;
  bool eof = false;

  if (iosize == 0) return true;

  if ((offset + size) % kPageSize != 0) {
    size_t last_page_len = (offset + size) % kPageSize;
    iov[0].iov_base = head.prev->data + last_page_len;
    acc_iosize = iov[0].iov_len = std::min(kPageSize - last_page_len, iosize);
    iovcnt = 1;
  }

  GrowBack(iosize);

  while (acc_iosize < iosize) {
    Page *pg = last_page->next;
    iov[iovcnt].iov_base = pg->data;
    iov[iovcnt].iov_len = std::min(iosize - acc_iosize, kPageSize);
    acc_iosize += iov[iovcnt].iov_len;
    iovcnt++;
    last_page = pg;
  }

again:
  ssize_t rs = callback(iov, iovcnt);
  if (rs < 0) {
    if (errno == EINTR) {
      goto again;
    } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
      rs = 0;
      goto shrink;
    } else {
      perror("readv");
      std::abort();
    }
  }

  if (rs == 0) eof = true;

shrink:
  ShrinkBack(iosize - rs);
  return !eof;
}

template <typename Func>
void IOBuffer::WriteTo(Func callback)
{
  size_t iosize = std::min(kBatchIOSize, size);
  struct iovec iov[kBatchIOSize / kPageSize + 1];
  int iovcnt = 0;
  size_t acc_iosize = 0;
  Page *last_page = &head;

  if (iosize == 0) return;

  if (offset != 0) {
    size_t first_page_len = offset;
    iov[0].iov_base = head.next->data + first_page_len;
    acc_iosize = iov[0].iov_len = std::min(kPageSize - first_page_len, iosize);
    iovcnt = 1;
    last_page = head.next;
  }

  while (acc_iosize < iosize) {
    Page *pg = last_page->next;
    iov[iovcnt].iov_base = pg->data;
    iov[iovcnt].iov_len = std::min(iosize - acc_iosize, kPageSize);
    acc_iosize += iov[iovcnt].iov_len;
    iovcnt++;
    last_page = pg;
  }

again:
  ssize_t rs = callback(iov, iovcnt);
  if (rs < 0) {
    if (errno == EINTR) {
      goto again;
    } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
      rs = 0;
      goto shrink;
    } else {
      perror("writev");
      std::abort();
    }
  }
shrink:
  ShrinkFront(rs);
}

size_t IOBuffer::Peek(void *data, size_t cnt)
{
  uint8_t *p = (uint8_t *) data;
  size_t acc = 0;
  WriteTo(
      [p, cnt, &acc](struct iovec *iov, int iovcnt) -> int {
        for (int i = 0; i < iovcnt && acc < cnt; i++) {
          size_t len = std::min(iov[i].iov_len, cnt - acc);
          memcpy(p + acc, iov[i].iov_base, len);
          acc += len;
        }
        // Don't call ShrinkFront() after WriteTo().
        errno = EAGAIN;
        return -1;
      });
  return acc;
}

void IOBuffer::Pop(void *data, size_t cnt)
{
  size_t acc = 0;
  uint8_t *p = (uint8_t *) data;
  while (acc < cnt) {
    WriteTo(
        [p, cnt, &acc](struct iovec *iov, int iovcnt) -> int {
          size_t perround_acc = 0;
          for (int i = 0; i < iovcnt && acc < cnt; i++) {
            size_t len = std::min(iov[i].iov_len, cnt - acc);
            memcpy(p + acc, iov[i].iov_base, len);
            acc += len;
            perround_acc += len;
          }
          return perround_acc;
        });
  }
}

void IOBuffer::Push(const void *data, size_t cnt)
{
  size_t acc = 0;
  uint8_t *p = (uint8_t *) data;
  while (acc < cnt) {
    ReadFrom([p, cnt, &acc](struct iovec *iov, int iovcnt) -> int {
        size_t perround_acc = 0;
        for (int i = 0; i < iovcnt && acc < cnt; i++) {
          size_t len = std::min(iov[i].iov_len, cnt - acc);
          memcpy(iov[i].iov_base, p + acc, len);
          acc += len;
          perround_acc += len;
        }
        return perround_acc;
      });
  }
}

BufferChannel::BufferChannel(size_t buffer_size)
    : capacity(buffer_size), bufsz(0), small_buffer(nullptr),
      large_buffer(nullptr), eof(false)
{
  if (capacity <= 1024)
    small_buffer = new uint8_t[capacity];
  else
    large_buffer = new IOBuffer(capacity + IOBuffer::kPageSize);
  rsleep_q.Init();
  wsleep_q.Init();
}

BufferChannel::~BufferChannel()
{
  delete [] small_buffer;
}

void BufferChannel::MemBufferRead(void *data, size_t cnt)
{
  memcpy(data, small_buffer, cnt);
  memmove(small_buffer, small_buffer + cnt, bufsz - cnt);
  bufsz -= cnt;
}

void BufferChannel::IOBufferRead(void *data, size_t cnt)
{
  large_buffer->Pop(data, cnt);
  bufsz -= cnt;
}


static void CheckCapacity(const char *op, size_t capacity, size_t cnt, size_t bufsz)
{
  if (capacity < cnt) {
    fprintf(stderr, "Cannot %s %ld with size %ld\n", op, cnt, bufsz);
    std::abort();
  }
}

void BufferChannel::BeginPeek()
{
  mutex.lock();
}

size_t BufferChannel::Peek(void *data, size_t cnt)
{
  if (small_buffer) {
    size_t len = std::min(cnt, bufsz);
    memcpy(data, small_buffer, len);
    return len;
  }

  if (large_buffer) {
    return large_buffer->Peek(data, cnt);
  }

  std::abort();
}

void BufferChannel::Skip(size_t skip)
{
  if (small_buffer) {
    if (bufsz >= skip) bufsz -= skip;
  }
  if (large_buffer) {
    large_buffer->ShrinkFront(skip);
  }
}

void BufferChannel::EndPeek()
{
  mutex.unlock();
}

bool BufferChannel::Read(void *data, size_t cnt)
{
  CheckCapacity(__FUNCTION__, capacity, cnt, bufsz);
  mutex.lock();
  while (bufsz < cnt) {
    if (eof) {
      mutex.unlock();
      return false;
    }

    Scheduler::Current()->RunNext(Scheduler::SleepState, &rsleep_q, &mutex);
    mutex.lock();
  }

  if (small_buffer) MemBufferRead(data, cnt);
  if (large_buffer) IOBufferRead(data, cnt);

  NotifyAll(&wsleep_q);
  mutex.unlock();
  return true;
}

void BufferChannel::MemBufferWrite(const void *data, size_t cnt)
{
  memcpy(small_buffer + bufsz, data, cnt);
  bufsz += cnt;
}

void BufferChannel::IOBufferWrite(const void *data, size_t cnt)
{
  large_buffer->Push(data, cnt);
  bufsz += cnt;
}

bool BufferChannel::Write(const void *data, size_t cnt)
{
  CheckCapacity(__FUNCTION__, capacity, cnt, bufsz);
  mutex.lock();
  if (eof) {
    mutex.unlock();
    return false;
  }
  while (capacity - bufsz < cnt) {
    Scheduler::Current()->RunNext(Scheduler::SleepState, &wsleep_q, &mutex);
    mutex.lock();
  }

  if (small_buffer) MemBufferWrite(data, cnt);
  if (large_buffer) IOBufferWrite(data, cnt);

  NotifyAll(&rsleep_q);
  mutex.unlock();
  return true;
}

void BufferChannel::NotifyAll(Scheduler::Queue *q)
{
  ScheduleEntity *ent = q->next;
  while (ent != q) {
    auto next = ent->next;
    Routine *r = (Routine *) ent;
    r->scheduler()->WakeUp(r);
    ent = next;
  }
}

void BufferChannel::Flush(bool async)
{
  if (async) return;

  mutex.lock();
  while (bufsz > 0) {
    Scheduler::Current()->RunNext(Scheduler::SleepState, &wsleep_q, &mutex);
    mutex.lock();
  }
  mutex.unlock();
}

void BufferChannel::Close()
{
  std::lock_guard<std::mutex> _(mutex);
  eof = true;
}

void TcpSocket::InitTcpSocket(size_t in_buffer_size, size_t out_buffer_size, int fd,
                              Scheduler *scheduler)
{
  mask = 0;
  sched = scheduler;
  pinned = false;
  ready = false;
  has_error = false;

  for (int i = 0; i < kNrQueues; i++) {
    wait_queues[i].ready = false;
    wait_queues[i].q.Init();
    omit_lock_queues[i] = false;
  }
  this->fd = fd;

  // Setting the socket to nonblock mode
  if (::fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK) < 0) {
    perror("fcntl");
    std::abort();
  }

  if (!sched)
    sched = go::Scheduler::Current();

  network_event_source()->AddSocket(this);

  in_chan = new TcpInputChannel(in_buffer_size, this);
  out_chan = new TcpOutputChannel(out_buffer_size, this);
}

TcpSocket::TcpSocket(size_t in_buffer_size, size_t out_buffer_size,
                     int domain, Scheduler *sched)
    : Event(0, NetworkEventSourceType)
{
  InitTcpSocket(in_buffer_size, out_buffer_size, ::socket(domain, SOCK_STREAM, 0), sched);
  this->domain = domain;
}

TcpSocket::TcpSocket()
    : Event(0, NetworkEventSourceType), sockaddrlen(0), in_chan(nullptr), out_chan(nullptr)
{}

TcpSocket::~TcpSocket()
{
  delete in_chan;
  delete out_chan;
}

void TcpSocket::OnError()
{
  has_error = true;
  network_event_source()->RemoveSocket(this);
}

bool TcpSocket::Pin()
{
  if (sched == go::Scheduler::Current()) {
    pinned = true;
    return true;
  }
  return false;
}

bool TcpSocket::FillSockAddr(CommonInetAddr &sockaddr, socklen_t &sockaddrlen, int domain, std::string address, int port)
{
  // TODO: Should use getaddrinfo() to query the DNS.
  union {
    struct in_addr addr4;
    struct in6_addr addr6;
  } addr;

  if (inet_pton(domain, address.c_str(), &addr) < 0) {
    return false;
  }

  if (domain == AF_INET) {
    sockaddr.sockaddr4.sin_family = AF_INET;
    sockaddr.sockaddr4.sin_addr = addr.addr4;
    sockaddr.sockaddr4.sin_port = htons(port);
    sockaddrlen = sizeof(struct sockaddr_in);
  } else if (domain == AF_INET6) {
    sockaddr.sockaddr6.sin6_family = AF_INET6;
    sockaddr.sockaddr6.sin6_addr = addr.addr6;
    sockaddr.sockaddr6.sin6_port = htons(port);
    sockaddrlen = sizeof(struct sockaddr_in6);
  }
  return true;
}

bool TcpSocket::Connect(std::string address, int port)
{
  FillSockAddr(sockaddr, sockaddrlen, domain, address, port);

again:
  if (::connect(fd, (struct sockaddr *) &sockaddr, sockaddrlen) < 0) {
    if (errno == EINTR) {
      goto again;
    } else if (errno == EINPROGRESS) {
      Wait(QueueEnum::WriteQueue);
      goto again;
    } else {
      perror("connect");
      return false;
    }
  }
  return true;
}

bool TcpSocket::Bind(std::string address, int port)
{
  FillSockAddr(sockaddr, sockaddrlen, domain, address, port);

again:
  if (::bind(fd, (struct sockaddr *) &sockaddr, sockaddrlen) < 0) {
    perror("bind");
    return false;
  }
  return true;
}

bool TcpSocket::Listen(int backlog)
{
  if (::listen(fd, backlog) < 0) {
    perror("listen");
    return false;
  }
  return true;
}

void TcpSocket::Close()
{
again:
  if (::close(fd) < 0) {
    if (errno == EINTR) {
      goto again;
    } else {
      perror("close");
    }
  }
}

void TcpSocket::EnableReuse()
{
  int enable = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enable, 4) < 0) {
    perror("setsockopt SO_REUSEADDR");
  }
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &enable, 4) < 0) {
    perror("setsockopt SO_REUSEPORT");
  }
}

TcpSocket *TcpSocket::Accept(size_t in_buffer_size, size_t out_buffer_size)
{
  TcpSocket *result = new TcpSocket();
  int client_fd = 0;
again:
  client_fd = ::accept(fd, (struct sockaddr *) &result->sockaddr, &result->sockaddrlen);
  if (client_fd < 0) {
    if (errno == EINTR) {
      goto again;
    } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
      Wait(QueueEnum::ReadQueue);
      goto again;
    } else {
      perror("accept");
      delete result;
      return nullptr;
    }
  }
  result->InitTcpSocket(in_buffer_size, out_buffer_size, client_fd, sched);
  return result;
}

void TcpSocket::Wait(int qid)
{
  std::mutex *l = wait_queue_lock(qid);
  if (l) l->lock();
  network_event_source()->WatchSocket(this, qid == ReadQueue ? EPOLLIN : EPOLLOUT);
  auto *exec_sched = go::Scheduler::Current();
  exec_sched->RunNext(go::Scheduler::SleepState, &wait_queues[qid].q, l);
}

bool TcpSocket::NotifyAndIO(int qid)
{
  auto &wq = wait_queues[qid];
  bool should_unwatch = false;
  bool done = false;
  std::mutex *l = wait_queue_lock(qid);
  if (l) l->lock();
  if (!ready) {
    if (qid == ReadQueue) {
      // TODO: accept?
    } else if (qid == WriteQueue) {
      ready = true;
    }
  } else {
    if (qid == ReadQueue) {
      while (!done && !should_unwatch) {
        if (!wq.buffer->ReadFrom([this, &done](struct iovec *iov, size_t iovcnt) {
              auto rs = ::readv(this->fd, iov, iovcnt);
              if (rs < 0 && errno == EAGAIN)
                done = true;
              return rs;
            })) {
          fprintf(stderr, "ReadFrom() return false\n");
          has_error = true;
          should_unwatch = true;
        }
        if (wq.buffer->max_grow_back_space() == 0) should_unwatch = true;
      }
    } else if (qid == WriteQueue) {
      while (!done && !should_unwatch) {
        wq.buffer->WriteTo([this, &done](struct iovec *iov, size_t iovcnt) {
            auto rs = ::writev(this->fd, iov, iovcnt);
            if (rs < 0 && errno == EAGAIN)
                done = true;
            return rs;
          });
        if (wq.buffer->buffer_size() == 0) should_unwatch = true;
      }
    }
  }
  auto q = &wq.q;
  auto *ent = q->next;
  while (ent != q) {
    auto next = ent->next;
    Routine *r = (Routine *) ent;
    // fprintf(stderr, "wake up %p\n", r);
    r->scheduler()->WakeUp(r);
    ent = next;
  }
  if (l) l->unlock();
  return should_unwatch;
}

std::string TcpSocket::host() const
{
  if (sockaddrlen == sizeof(struct sockaddr_in)) {
    return std::string(inet_ntoa(sockaddr.sockaddr4.sin_addr));
  } else if (sockaddrlen == sizeof(struct sockaddr_in6)) {
    // TODO:
    return std::string("IPv6 addr not implemented");
  } else {
    fputs("Cannot recognize the addrlen\n", stderr);
    std::abort();
  }
}

void TcpSocket::Migrate(Scheduler *to_sched)
{
  network_event_source()->RemoveSocket(this);
  sched = to_sched;
  network_event_source()->AddSocket(this);
}

NetworkEventSource::NetworkEventSource(Scheduler *sched)
    : EventSource(sched)
{
}

void NetworkEventSource::AddSocket(TcpSocket *sock)
{
  struct epoll_event e{0, {sock}};
  if (::epoll_ctl(sched_epoll(), EPOLL_CTL_ADD, sock->fd, &e) < 0) {
    perror("epoll_add");
    std::abort();
  }
}

void NetworkEventSource::RemoveSocket(TcpSocket *sock)
{
  // fprintf(stderr, "remove epoll mod mask %d\n", go::Scheduler::CurrentThreadPoolId());
  struct epoll_event e{0, {sock}};
  if (::epoll_ctl(sched_epoll(), EPOLL_CTL_DEL, sock->fd, &e) < 0) {
    perror("epoll_remove");
    std::abort();
  }
}

void NetworkEventSource::WatchSocket(go::TcpSocket *sock, uint32_t mask)
{
  if (!sock->pinned) sock->mask_mutex.lock();
  std::lock_guard<std::mutex> _(sock->mask_mutex, std::adopt_lock);

  auto oldmask = sock->mask;
  sock->mask |= mask;
  if (oldmask == sock->mask) return;

  // fprintf(stderr, "watch epoll mod mask %d on %d\n", sock->mask, go::Scheduler::CurrentThreadPoolId());
  struct epoll_event e{sock->mask, {sock}};
  if (::epoll_ctl(sched_epoll(), EPOLL_CTL_MOD, sock->fd, &e) < 0) {
    perror("epoll_mod");
    std::abort();
  }
}

void NetworkEventSource::UnWatchSocket(go::TcpSocket *sock, uint32_t mask)
{
  if (!sock->pinned) sock->mask_mutex.lock();
  std::lock_guard<std::mutex> _(sock->mask_mutex, std::adopt_lock);

  auto oldmask = sock->mask;
  sock->mask &= ~mask;
  if (oldmask == sock->mask) return;

  // fprintf(stderr, "unwatch epoll mod mask %d on %d\n", sock->mask, go::Scheduler::CurrentThreadPoolId());
  struct epoll_event e{sock->mask, {sock}};
  if (::epoll_ctl(sched_epoll(), EPOLL_CTL_MOD, sock->fd, &e) < 0) {
    perror("epoll_mod");
    std::abort();
  }
}

void NetworkEventSource::OnEvent(Event *evt)
{
  TcpSocket *sock = (TcpSocket *) evt;
  uint32_t mask = 0;
  if (evt->mask & EPOLLIN) {
    if (sock->NotifyAndIO(TcpSocket::ReadQueue))
      mask |= EPOLLIN;
  }

  if (evt->mask & EPOLLOUT) {
    if (sock->NotifyAndIO(TcpSocket::WriteQueue))
      mask |= EPOLLOUT;
  }

  if (mask) UnWatchSocket(sock, mask);

  if (evt->mask & EPOLLERR) {
    sock->OnError();
  }
}

TcpInputChannel::TcpInputChannel(size_t buffer_size, go::TcpSocket *sock)
    : sock(sock)
{
  q = &sock->wait_queues[TcpSocket::ReadQueue];
  q->buffer = new IOBuffer(buffer_size + IOBuffer::kPageSize);
}

TcpInputChannel::~TcpInputChannel()
{
  delete q->buffer;
}

TcpOutputChannel::TcpOutputChannel(size_t buffer_size, go::TcpSocket *sock)
    : sock(sock)
{
  q = &sock->wait_queues[TcpSocket::WriteQueue];
  q->buffer = new IOBuffer(buffer_size + IOBuffer::kPageSize);
}

TcpOutputChannel::~TcpOutputChannel()
{
  delete q->buffer;
}

void TcpInputChannel::OpportunisticReadFromNetwork()
{
  int fd = sock->fd;
  bool success = !sock->has_error;
  bool done = false;
  while (success && !done && q->buffer->max_grow_back_space() > 0) {
    success = q->buffer->ReadFrom(
        [fd, &done](struct iovec *iov, size_t iovcnt) {
          auto rs = ::readv(fd, iov, iovcnt);
          if (rs < 0 && errno == EAGAIN)
            done = true;
          return rs;
        });
  }
}

void TcpInputChannel::BeginPeek()
{
  std::mutex *l = sock->wait_queue_lock(TcpSocket::ReadQueue);
  if (l) l->lock();
}

size_t TcpInputChannel::Peek(void *data, size_t cnt)
{
  if (q->buffer->buffer_size() < cnt) {
    OpportunisticReadFromNetwork();
  }

  return q->buffer->Peek(data, cnt);
}

void TcpInputChannel::Skip(size_t skip)
{
  q->buffer->ShrinkFront(skip);
}

void TcpInputChannel::EndPeek()
{
  std::mutex *l = sock->wait_queue_lock(TcpSocket::ReadQueue);
  if (l) l->unlock();
}

bool TcpInputChannel::Read(void *data, size_t cnt)
{
  CheckCapacity(__FUNCTION__, q->buffer->capacity(), cnt, q->buffer->buffer_size());
  auto q = &sock->wait_queues[TcpSocket::ReadQueue];
  std::mutex *l = sock->wait_queue_lock(TcpSocket::ReadQueue);
  if (l) l->lock();

  while (q->buffer->buffer_size() < cnt) {
    OpportunisticReadFromNetwork();

    if (q->buffer->buffer_size() >= cnt) break;
    if (sock->has_error) {
      if (l) l->unlock();
      return false;
    }

    sock->network_event_source()->WatchSocket(sock, EPOLLIN);
    Scheduler::Current()->RunNext(Scheduler::SleepState, &q->q, l);
    if (l) l->lock();
  }

  q->buffer->Pop(data, cnt);
  if (l) l->unlock();

  return true;
}

bool TcpOutputChannel::Write(const void *data, size_t cnt)
{
  CheckCapacity(__FUNCTION__, q->buffer->capacity(), cnt, q->buffer->buffer_size());
  auto q = &sock->wait_queues[TcpSocket::WriteQueue];
  std::mutex *l = sock->wait_queue_lock(TcpSocket::WriteQueue);
  if (l) l->lock();

  while (q->buffer->max_grow_back_space() < cnt) {
    if (sock->has_error) {
      if (l) l->unlock();
      return false;
    }

    int fd = sock->fd;
    bool done = false;
    while (q->buffer->buffer_size() > 0 && !done) {
      q->buffer->WriteTo([fd, &done](struct iovec *iov, size_t iovcnt) {
          auto rs = ::writev(fd, iov, iovcnt);
          if (rs < 0 && errno == EAGAIN)
            done = true;
          return rs;
        });
    }

    if (q->buffer->max_grow_back_space() >= cnt) break;

    sock->network_event_source()->WatchSocket(sock, EPOLLOUT);
    Scheduler::Current()->RunNext(Scheduler::SleepState, &q->q, l);
    if (l) l->lock();
  }

  q->buffer->Push(data, cnt);
  if (l) l->unlock();
  return true;
}

void TcpOutputChannel::Flush(bool async)
{
  auto q = &sock->wait_queues[TcpSocket::WriteQueue];
  if (async && q->buffer->buffer_size() == 0) return;

  std::mutex *l = sock->wait_queue_lock(TcpSocket::WriteQueue);
  if (l) l->lock();
  while (q->buffer->buffer_size() > 0) {
    if (sock->has_error) {
      if (l) l->unlock();
      return;
    }

    int fd = sock->fd;
    bool done = false;
    while (q->buffer->buffer_size() > 0 && !done) {
      q->buffer->WriteTo([fd, &done](struct iovec *iov, size_t iovcnt) {
        auto rs = ::writev(fd, iov, iovcnt);
        if (rs < 0 && errno == EAGAIN)
          done = true;
        return rs;
      });
    }
    if (q->buffer->buffer_size() == 0) break;
    if (async) break;

    sock->network_event_source()->WatchSocket(sock, EPOLLOUT);
    Scheduler::Current()->RunNext(Scheduler::SleepState, &q->q, l);
    if (l) l->lock();
  }
  if (l) l->unlock();
}

void TcpOutputChannel::Close()
{
  sock->Close();
}

}
