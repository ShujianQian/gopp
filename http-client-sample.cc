#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>
#include <unistd.h>
#include <sys/eventfd.h>

#include "gopp.h"
#include "channels.h"

class HttpClient : public go::Routine {
  std::string host;
  int port;
 public:
  HttpClient(std::string host, int port) : host(host), port(port) {}
  void Run() final;
};

static int done = eventfd(0, EFD_SEMAPHORE);

void HttpClient::Run()
{
  auto *socket = new go::TcpSocket(256 << 20, 256 << 10);
  if (!socket->Pin()) {
    fprintf(stderr, "Cannot pin\n");
  }
  socket->Connect(host, port);
  auto *input = socket->input_channel();
  auto *output = socket->output_channel();
  FILE *fp = fopen("dump", "w");

  std::stringstream ss;

  ss << "GET /test/8/dolly-net.0.dump HTTP/1.0\r\n\r\n";

  output->Write(ss.str().c_str(), ss.str().length());
  output->Flush();

  int lcnt = 0;
  bool content_mode = false;
  while (true) {
    char ch;
    if (!input->Read(&ch, 1)) {
      break;
    }

    if (content_mode) {
      fputc(ch, fp);
      continue;
    }
    if (ch == '\r') {
      continue;
    } else if (ch == '\n') {
      if (lcnt == 0) content_mode = true;
      lcnt = 0;
      putchar(ch);
      continue;
    } else {
      lcnt++;
      putchar(ch);
    }
  }

  fclose(fp);
  socket->Close();
  delete socket;
  uint64_t u = 1;
  ::write(done, &u, sizeof(uint64_t));
}

int main(int argc, char *argv[])
{
  go::InitThreadPool(1);
  go::GetSchedulerFromPool(1)->WakeUp(new HttpClient(argv[1], atoi(argv[2])));
  uint64_t u = 0;
  ::read(done, &u, sizeof(uint64_t));
  go::WaitThreadPool();
  return 0;
}
