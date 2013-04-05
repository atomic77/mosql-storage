#ifndef _SM_H_
#define _SM_H_

#include "dsmDB_priv.h"
#include <event2/bufferevent.h>
#include <libpaxos/config_reader.h>

int sm_init(struct config *lp_config, struct event_base *base);

int sm_cleanup();

// Returns NULL if value is not local, TM must retry later
// Return a null_val (a val of size 0) if k does not exist
// otherwise returns a valid val*
val* sm_get(key* k, int version);

int sm_put(key* k, val* v);

void sm_recovery();

void sm_dump_storage(char* path, int version);

#endif /* _SM_H_ */
