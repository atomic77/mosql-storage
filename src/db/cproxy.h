#ifndef _CPROXY_H_
#define _CPROXY_H_

#include "dsmDB_priv.h"
#include <event2/event.h>

typedef void(*cproxy_commit_cb)(tr_id*, int);

int cproxy_init(const char* paxos_config, struct event_base *base);
int cproxy_submit(char* value, size_t size, cproxy_commit_cb cb);
int cproxy_submit_join(int id, char* address, int port);
int cproxy_current_st();
void cproxy_cleanup();

#endif /* _CPROXY_H_ */
