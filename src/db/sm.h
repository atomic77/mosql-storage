#ifndef _SM_H_
#define _SM_H_

#include "dsmDB_priv.h"

int sm_init();

int sm_cleanup();

// Returns NULL if value is not local, TM must retry later
// Return a null_val (a val of size 0) if k does not exist
// otherwise returns a valid val*
val* sm_get(key* k, int version);

int sm_put(key* k, val* v);

void sm_recovery();

void sm_dump_storage(char* path, int version);

#endif /* _SM_H_ */
