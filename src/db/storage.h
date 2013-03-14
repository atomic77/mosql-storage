#ifndef _STORAGE_H_
#define _STORAGE_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "dsmDB_priv.h"
#include "peer.h"

int storage_init(void);

int storage_init2(consistent_hash chash);

void storage_free();

val* storage_get(key* k, int max_ver);

int storage_put(key* k, val* v, int local, int force_cache);

long storage_get_current_size();

long storage_key_count();

long storage_val_count();

long storage_gc_count();

void storage_gc_at_least(int bytes);

int storage_iterate(int version, void (iter)(key*, val*, void*), void* arg);

void storage_gc_start();

void storage_gc_stop();

#ifdef __cplusplus
}
#endif

#endif /* _STORAGE_H_ */
