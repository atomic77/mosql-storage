#ifndef _REMOTE_H_
#define _REMOTE_H_

#include "dsmDB_priv.h"


typedef void(*sm_get_cb)(key*, val*, void*);


int remote_init();

int remote_cleanup();

void remote_get(key* k, int version, sm_get_cb cb, void* arg);

void remote_start_recovery();

void remote_print_stats();

#endif /*_REMOTE_H_ */
