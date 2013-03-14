#ifndef _TRANSACTION_H_
#define _TRANSACTION_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "dsmDB_priv.h"
#include "hashtable.h"
#include "cproxy.h"

typedef void(*transaction_cb)(key*, val*, void*);

typedef struct transaction_t {
    int   st;
    tr_id id;
	short seqn;
	int remote_count;
    struct hashtable* rs;
    struct hashtable* ws;
	transaction_cb get_cb;
	transaction_cb put_cb;
	void* cb_arg;
	val* cb_val;
	int never_set;
} transaction;


transaction* transaction_new();

void transaction_clear(transaction* t);

void transaction_clear_writeset(transaction* t);
void transaction_clear_readset(transaction* t);

void transaction_destroy(transaction* t);

val* transaction_get(transaction* t, key* k);

int transaction_put(transaction* t, key* k, val* v);

int transaction_commit(transaction* t, int id, cproxy_commit_cb cb);

int transaction_read_only(transaction* t);

int transaction_serialize(transaction* t, tr_submit_msg* msg, int max_size);

void transaction_set_get_cb(transaction* t, transaction_cb cb, void* arg);

int transaction_remote_count(transaction* t);


#ifdef __cplusplus
}
#endif

#endif /* _TRANSACTION_H_ */
