#ifndef CHANNELS_H
#define CHANNELS_H

#include <arpa/inet.h>
#include "gopp.h"

namespace go {

class IOBuffer;

class BufferChannel : public InputChannel, public OutputChannel {
  std::mutex mutex;
  size_t capacity;
  size_t bufsz;
  uint8_t *small_buffer;
  IOBuffer *large_buffer;
  Scheduler::Queue rsleep_q;
  Scheduler::Queue wsleep_q;
  bool eof;
 public:
  BufferChannel(size_t buffer_size);
  ~BufferChannel();

  bool Read(void *data, size_t cnt) final;
  bool Write(const void *data, size_t cnt) final;
  void Flush() final;
  void Close() final;
 private:
  void NotifyAll(Scheduler::Queue *q);

  void MemBufferRead(void *data, size_t cnt);
  void MemBufferWrite(const void *data, size_t cnt);
  void IOBufferRead(void *data, size_t cnt);
  void IOBufferWrite(const void *data, size_t cnt);
};

class NetworkEventSource;
class TcpInputChannel;
class TcpOutputChannel;

class TcpSocket : public Event {
  friend class TcpInputChannel;
  friend class TcpOutputChannel;

  static const int kNrQueues = 2;
  struct WaitQueue {
    std::mutex mutex;
    bool ready;
    Scheduler::Queue q;
    IOBuffer *buffer;
  };

  WaitQueue wait_queues[kNrQueues];

  enum QueueEnum : int {
    ReadQueue,
    WriteQueue,
  };

  std::mutex mask_mutex;
  uint32_t mask;

  Scheduler *sched;
  bool pinned;
  bool ready;
  bool has_error;
  int domain;

  union {
    struct sockaddr_in sockaddr4;
    struct sockaddr_in6 sockaddr6;
  } sockaddr;
  socklen_t sockaddrlen;

  TcpInputChannel *in_chan;
  TcpOutputChannel *out_chan;

 public:
  TcpSocket(size_t in_buffer_size, size_t out_buffer_size,
            int domain = AF_INET, Scheduler *sched = nullptr);
  virtual ~TcpSocket();

  bool Pin();
  bool Connect(std::string address, int port);
  void Accept(); // TODO:
  void Close();

  TcpInputChannel *input_channel() { return in_chan; }
  TcpOutputChannel *output_channel() { return out_chan; }

  virtual void OnError();
 private:
  void Wait(int qid);
  bool NotifyAndIO(int qid);
  NetworkEventSource *network_event_source() {
    return (NetworkEventSource *)
        sched->event_source(EventSourceTypes::NetworkEventSourceType);
  }
  std::mutex *wait_queue_lock(int qid) {
    std::mutex *l = nullptr;
    if (!pinned) {
      l = &wait_queues[qid].mutex;
    } else {
      if (go::Scheduler::Current() != sched) {
        fprintf(stderr, "Pinned socket does not support concurrent access!");
        std::abort();
      }
    }
    return l;
  }
  friend class NetworkEventSource;
};

class NetworkEventSource : public EventSource {
 public:
  NetworkEventSource(Scheduler *sched);

  void OnEvent(Event *evt) final;
  bool ReactEvents() final { return false; };

  void AddSocket(TcpSocket *sock);
  void RemoveSocket(TcpSocket *sock);
  void WatchSocket(TcpSocket *sock, uint32_t mask);
  void UnWatchSocket(TcpSocket *sock, uint32_t mask);
};

class TcpInputChannel : public InputChannel {
  friend class TcpSocket;
  TcpSocket *sock;
  TcpSocket::WaitQueue *q;

  TcpInputChannel(size_t buffer_size, TcpSocket *sock);
  virtual ~TcpInputChannel();
 public:

  bool Read(void *data, size_t cnt) final;
};

class TcpOutputChannel : public OutputChannel {
  friend class TcpSocket;
  TcpSocket *sock;
  TcpSocket::WaitQueue *q;

  TcpOutputChannel(size_t buffer_size, TcpSocket *sock);
  virtual ~TcpOutputChannel();

 public:
  bool Write(const void *data, size_t cnt) final;
  void Flush() final;
  void Close() final;
};

}

#endif /* CHANNELS_H */
