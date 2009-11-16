#ifndef __SCRIBE_C_H__
#define __SCRIBE_C_H__

#ifdef __cplusplus
extern "C" {
#endif

typedef struct scribe_t {
  char *host;
  int   port;

  //  ScribeClient *scribeClient;
  void *scribeClient;

  //TTransport   *transport;
  void *transport;
} scribe_t, *scribe_t_ptr;

int scribe_open(scribe_t *p, const char *host, const int port);
int scribe_write(scribe_t *p, const char *category, const char *buf);
int scribe_close(scribe_t *p);

#ifdef __cplusplus
}
#endif

#endif
