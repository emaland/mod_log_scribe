#include <stdlib.h>
#include "scribe_c.h"

#define DEFAULT_HOST "localhost"
#define DEFAULT_CATEGORY "default"

int main(void)
{
  scribe_t *scribe = calloc(1, sizeof(scribe_t));
  scribe_open(scribe, DEFAULT_HOST", 1463);
  scribe_write(scribe, DEFAULT_CATEGORY, "this is a message: foo");
  scribe_close(scribe);
  return 0;
}
