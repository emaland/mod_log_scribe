/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/* ====================================================================
 * mod_log_scribe.c - Scribe logging interface for Apache
 * Version 20090928.01
 * Eric Maland <eric@twitter.com>
 * ====================================================================
 */

#include "apr_strings.h"
#include "apr_lib.h"
#include "apr_hash.h"
#include "apr_optional.h"
#include "apr_reslist.h"

#define APR_WANT_STRFUNC
#include "apr_want.h"

#include "ap_config.h"
#include "mod_log_config.h"
#include "httpd.h"
#include "http_config.h"
#include "http_log.h"
#include "http_protocol.h"
#include "util_time.h"

#if APR_HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_LIMITS_H
#include <limits.h>
#endif

/* Simple C interface wrapper around scribe/thrift API */
#include "scribe_c.h"

module AP_MODULE_DECLARE_DATA log_scribe_module;

/* scribe log config */
typedef struct {
  const char *fallbackURI;
  int timeoutInterval;
  int retryInterval;
  int logLocally;
} scribe_log_config;

/* single log target */
typedef struct {
#if APR_HAS_THREADS
    apr_reslist_t *scribes; /* connection pool */
#else
    scribe_t *scribe;   /* no threads, no pool */
#endif
    char *uri;
    char *host;         
    int port;
    char *category;
    int connectTimeout;

    const char *fallbackURI;
    int fallingback;
    int retryTimeout;
  
    int localonly; /* this store is not a scribe store */
    void *normal_handle; /* apache mod_log_config handle */
} scribe_log;

static apr_hash_t *scribe_hash;
static ap_log_writer_init *normal_log_writer_init = NULL;
static ap_log_writer *normal_log_writer = NULL;

/* 
 * open a new scribe connection
 * called automatically by scribe_log.scribes pool
 */
static apr_status_t open_scribe_connection(void **resource, void *param, apr_pool_t *p)
{
  scribe_log *l = param;
  scribe_t *scribe = apr_palloc(p, sizeof(scribe_t));

  fprintf(stderr, "opening cxn\n");
  if(scribe_open(scribe, l->host, l->port)) {
    ap_log_perror(APLOG_MARK, APLOG_CRIT, 0, p, "open scribe log FAILED %s:%d: %s", l->host, l->port, strerror(errno));
    /* Failure - if we have a fallback, try it.  If not, Fail */
    if(l->fallbackURI != NULL && l->fallingback == 0) {
      l->host = ""; /* parse uri etc. */
      l->port = 0;  /* parse port etc. */
      l->fallingback = 1;

      ap_log_perror(APLOG_MARK, APLOG_NOTICE, 0, p, "Attempting fallback to %s", l->fallbackURI);
      /* potential for infinite loop - fallingback == 1 should prevent */
      return open_scribe_connection(resource, param, p);
    }
    *resource = NULL;
    fprintf(stderr, "failed to open scribe connection\n");
    return !APR_SUCCESS;
  } else {
    *resource = scribe;
    return APR_SUCCESS;
  }
}

/* close a scribe connection, called automatically by scribe_log.scribes */
static apr_status_t close_scribe_connection(void *resource, void *param, apr_pool_t *p)
{
#warning [emaland] possible leakage - cleanup here
  if (resource) {
    scribe_close((scribe_t*)resource);
  }

  return OK;
}

/* setup a new log target, called from mod_log_config */
static void *scribe_log_writer_init(apr_pool_t *p, server_rec *s, const char* name)
{
    scribe_log *l;
    char *uri;
    char *c = NULL;
    //    apr_status_t as;
    scribe_log_config *conf = ap_get_module_config(s->module_config,
                                                   &log_scribe_module);

    int scribeWriter = 1;

    if(name != NULL && strstr(name, "scribe") == NULL)
      scribeWriter = 0;

    if(!scribeWriter && conf->logLocally == 0)
      return NULL;

    if (! (l = apr_hash_get(scribe_hash, name, APR_HASH_KEY_STRING))) {
        l = apr_palloc(p, sizeof(scribe_log));

#if APR_HAS_THREADS
        /* no initial connections created here in order to avoid opening 
           things in the root process */
        if (apr_reslist_create(&l->scribes, 0, 5, 50, 100,
                               open_scribe_connection, 
                               close_scribe_connection, l, p) != APR_SUCCESS) {
            return NULL;
        }
#else
        l->scribe = NULL;
#endif
        l->uri = apr_pstrdup(p, name); /* keep our full name */
        uri = apr_pstrdup(p,name);     /* keep a copy for config */

        l->host = "defaulthost";
        l->port = 1463;
        l->category = "default";

        l->connectTimeout = conf->timeoutInterval;
        l->fallbackURI = conf->fallbackURI;
        l->retryTimeout = conf->retryInterval;
        l->fallingback = 0;

        if(scribeWriter != 0) {
          c = ap_strrchr(uri, ':');
          if(c != NULL) {
            if(c != uri+6) {
              l->port = apr_atoi64(c+1);
              *c = '\0';
            }
          } else {
            l->port = 1463;
          }

          c = ap_strrchr(uri, '@');
          if(c != NULL) {
            *c++ = '\0';
            l->host = c;
          }

          c = ap_strrchr(uri, ':');
          if(c != NULL) {
            *c++ = '\0';
            l->category = c;
          }
          l->localonly = 0;
        } else {
          l->localonly = 1;
          l->normal_handle = normal_log_writer_init(p, s, name);
        }
        apr_hash_set(scribe_hash, name, APR_HASH_KEY_STRING, l);
    }

    /* this is an L not a *1* (the number one).  should rename that.  */
    return l;
}

/* log a request */
static apr_status_t scribe_log_writer(request_rec *r,
                                      void *handle,
                                      const char **strs,
                                      int *strl,
                                      int nelts,
                                      apr_size_t len)
{
    scribe_t *scribe;
    scribe_log *l = (scribe_log*)handle;
    //    scribe_log_config *conf;

    if(l->localonly != 0 && l->normal_handle) {
      fprintf(stderr, "calling normal log writer\n");
      apr_status_t result = normal_log_writer(r, l->normal_handle, strs, strl, nelts, len);
      fprintf(stderr, "called normal log writer\n");
      return result;
    }

#if APR_HAS_THREADS
    if (apr_reslist_acquire(l->scribes, (void *)&scribe) != APR_SUCCESS) {
        scribe = NULL;
    }
#else
#warning [emaland] This will asplode - fix
    if ((! l->scribe) && (scribe_open((void*)&l->scribe, l->host, l->port) != 0)) {
        scribe = NULL;
    }
    else {
        scribe = l->scribe;
    }
#endif

    if(scribe == NULL) {
      return DECLINED;
    }

    {
    char *str;
    char *s;
    int i;
    apr_status_t rv;

    str = apr_palloc(r->pool, len + 1);

    for (i = 0, s = str; i < nelts; ++i) {
      memcpy(s, strs[i], strl[i]);
      s += strl[i];
    }
    str[len+1] = '\0';
    
    fprintf(stderr, str);
    rv = scribe_write(scribe, l->category, str);
    }

#if APR_HAS_THREADS
    if (scribe) {
        apr_reslist_release(l->scribes, scribe);
    }
#endif

    return OK;
}

static void *make_log_scribe_config(apr_pool_t *p, server_rec *s)
{
    scribe_log_config *conf = 
      (scribe_log_config *)apr_palloc(p, sizeof(scribe_log_config));

    conf->fallbackURI = NULL;     /* secondary scribe host */
    conf->logLocally = 1;         /* allow normal apache logging */
    conf->timeoutInterval = 2000; /* 2 seconds */
    conf->retryInterval = 5000;   /* 5 seconds */

    return conf;
}

static const char *logscribe_loglocally(cmd_parms *cmd, void *dcfg, const char *arg)
{
  scribe_log_config *conf = ap_get_module_config(cmd->server->module_config,
                                                 &log_scribe_module);
  
  conf->logLocally = (arg && strcmp(arg, "Off") == 1) ? 0 : 1;

  return OK;
}


static const char *logscribe_fallback(cmd_parms *cmd, void *dcfg, const char *arg)
{
    scribe_log_config *conf = ap_get_module_config(cmd->server->module_config,
                                                   &log_scribe_module);
    conf->fallbackURI = arg;

    // FIXME: [emaland] validate the URI
    return OK;
}


static const char *logscribe_timeout(cmd_parms *cmd, void *dcfg, const char *arg)
{
    scribe_log_config *conf = ap_get_module_config(cmd->server->module_config,
                                                   &log_scribe_module);
    
    if(arg)
      conf->timeoutInterval = apr_atoi64(arg);

    return OK;
}

static const char *logscribe_retry(cmd_parms *cmd, void *dcfg, const char *arg)
{
    scribe_log_config *conf = ap_get_module_config(cmd->server->module_config,
                                                   &log_scribe_module);

    conf->retryInterval = apr_atoi64(arg);

    return OK;
}

static const command_rec log_scribe_cmds[] =
  { 
    AP_INIT_TAKE1("ScribeFallback", logscribe_fallback, NULL, RSRC_CONF,
                  "Secondary scribe store to fall back to"),
    AP_INIT_TAKE1("ScribeTimeoutInterval", logscribe_timeout, NULL, RSRC_CONF,
                  "Scribe connection timeout in milliseconds"),
    AP_INIT_TAKE1("ScribeRetryInterval", logscribe_retry, NULL, RSRC_CONF,
                  "Time between retries connecting to primary Scribe store, "
                  "in milliseconds"),
    AP_INIT_TAKE1("ScribeLogLocally", logscribe_loglocally, NULL, RSRC_CONF,
                  "Whether to turn on the base apache logging system"),
    {NULL}
  };

static int log_scribe_pre_config(apr_pool_t *p, apr_pool_t *plog, apr_pool_t *ptemp)
{
  static APR_OPTIONAL_FN_TYPE(ap_log_set_writer_init) 
    *log_set_writer_init_fn = NULL;
  static APR_OPTIONAL_FN_TYPE(ap_log_set_writer) *log_set_writer_fn = NULL;

  log_set_writer_init_fn = APR_RETRIEVE_OPTIONAL_FN(ap_log_set_writer_init);
  log_set_writer_fn = APR_RETRIEVE_OPTIONAL_FN(ap_log_set_writer);

  if(log_set_writer_init_fn && log_set_writer_fn) {
    if (!normal_log_writer_init) { 
      // FIXME: [emaland] add some error here if we can't load mod_log_config
      // Or maybe just warn and turn off local logging by default.  ???
      module *mod_log_config = ap_find_linked_module("mod_log_config.c"); 
      (void)mod_log_config; /* avoid annoying compiler warning */
      normal_log_writer_init = log_set_writer_init_fn(scribe_log_writer_init); 
      normal_log_writer = log_set_writer_fn(scribe_log_writer); 
    } 
  }

  return OK;
}

static apr_status_t log_scribe_child_exit(void *data)
{
    apr_pool_t *p = data;
    apr_hash_index_t *i;
    scribe_log *l;

    for (i = apr_hash_first(p, scribe_hash); i; i = apr_hash_next(i)) {
        apr_hash_this(i, NULL, NULL, (void*) &l);
#if APR_HAS_THREADS
        apr_reslist_destroy(l->scribes);
#else
        scribe_close(l->scribe);
#endif
#warning [emaland] clean up some memory foo here?        
    }
    return OK;
}

static void log_scribe_child_init(apr_pool_t *p, server_rec *s)
{
    apr_pool_cleanup_register(p, p, 
                              log_scribe_child_exit, log_scribe_child_exit);
}

static void register_hooks(apr_pool_t *p)
{
    /* register our log writer before mod_log_config starts */
    static const char *pre[] = { "mod_log_config.c", NULL }; 
    scribe_hash = apr_hash_make(p);
    ap_hook_pre_config(log_scribe_pre_config, pre, NULL, APR_HOOK_REALLY_FIRST);
    ap_hook_child_init(log_scribe_child_init, NULL, NULL, APR_HOOK_MIDDLE)
;
}

module AP_MODULE_DECLARE_DATA log_scribe_module =
{
    STANDARD20_MODULE_STUFF,
    NULL,                       /* create per-dir config */
    NULL,                       /* merge per-dir config */
    make_log_scribe_config,     /* server config */
    NULL,                       /* merge server config */
    log_scribe_cmds,            /* command apr_table_t */
    register_hooks              /* register hooks */
};
