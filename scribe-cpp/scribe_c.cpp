#include <string.h>

#include "scribe_c.h"

#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

#include "gen-cpp/scribe.h"

using namespace std;
using namespace facebook::thrift;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;

using namespace scribe::thrift;
using namespace scribe;

using namespace boost;

int 
scribe_open(scribe_t *p, const char *host, const int port)
{
  p->host = strdup(host);
  p->port = port;

  shared_ptr<TTransport> socket(new TSocket(p->host, p->port));
  shared_ptr<TTransport> transport(new TFramedTransport(socket));
  shared_ptr<TProtocol>  protocol(new TBinaryProtocol(transport));

  scribeClient *client = new scribeClient(protocol);

  transport->open();
  p->scribeClient = client;
  p->transport    = static_cast<void*>(transport.get());

  return 0;
}

int 
scribe_write(scribe_t *p, const char *category, const char *buf)
{
  LogEntry entry;
  entry.category = category;
  entry.message = buf;

  std::vector<LogEntry> msgs;
  msgs.push_back(entry);

  int result = ((scribeClient*)p->scribeClient)->Log(msgs);
  return result;
}

int 
scribe_close(scribe_t *p)
{
  ((TTransport*)p->transport)->close();
  delete (scribeClient*)p->scribeClient;
  memset(p, 0, sizeof(scribe_t));
  return 0;
}

