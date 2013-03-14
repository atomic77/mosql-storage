#ifndef _PROTOCOL_H_
#define _PROTOCOL_H_

#include "tapioca.h"

//#include <stdint.h>
//#include "../app/tapioca/bplustree_client.h"

void* protocol_open(const char* address, int port);
void protocol_close(void* ph);
int protocol_node_id(void* ph);
int protocol_client_id(void* p);
int protocol_get(void* ph, void* k, int ksize, void* v, int vsize);
int protocol_put(void* ph, void* k, int ksize, void* v, int vsize);
int protocol_commit(void* ph);
int protocol_rollback(void* ph);

int protocol_mget_int(void* ph, int n, int* keys, int* values);
int protocol_mput_int(void* ph, int n, int* keys, int* values);


int protocol_mget(void* ph, void* k, int ksize);
mget_result* protocol_mget_commit(void* ph);

int protocol_mput(void* ph, void* k, int ksize, void* v, int vsize);
int protocol_mput_commit(void* ph);

int protocol_mget_put(void* ph, void* k, int ksize, void* v, int vsize);
int protocol_mget_put_commit(void* ph);
int protocol_mput_commit_retry(void* ph, int times);

int protocol_btree_search(void* ph, long k, long* v);
int protocol_btree_insert(void* ph, long k, long v);
int protocol_btree_range(void* ph, long min, long max);
int protocol_btree_update(void* ph, long k, long v);

/********** B+Tree operations ******************************/

int protocol_bptree_initialize_bpt_session_no_commit(void *ph, uint16_t bpt_id,
		enum bptree_open_flags open_flags, uint32_t execution_id);
int protocol_bptree_initialize_bpt_session(void *ph, uint16_t bpt_id,
		enum bptree_open_flags open_flags);
int protocol_bptree_set_num_fields(void *ph,
		tapioca_bptree_id tbpt_id, int16_t num_fields);
int protocol_bptree_set_field_info(void *p,
		tapioca_bptree_id tbpt_id, int16_t field_num,
		int16_t field_sz, enum bptree_field_comparator comparator);
int protocol_bptree_insert(void *p, tapioca_bptree_id tbpt_id, void *k,
		int ksize, void *v, int vsize, enum bptree_insert_flags insert_flags);
int protocol_bptree_update(void *p, tapioca_bptree_id tbpt_id, void *k,
		int ksize, void *v, int vsize);
int protocol_bptree_search(void *p, tapioca_bptree_id tbpt_id, void *k,
		int ksize, void *v, int *vsize);
int protocol_bptree_index_first(void *p, tapioca_bptree_id tbpt_id, void *k,
		int *ksize, void *v, int *vsize);
int protocol_bptree_index_next(void *p, tapioca_bptree_id tbpt_id, void *k,
		int *ksize, void *v, int *vsize);
int protocol_bptree_index_next_mget(void *p, tapioca_bptree_id tbpt_id,
		bptree_mget_result **bmres, int16_t *rows);
int protocol_bptree_index_first_no_key(void *p, tapioca_bptree_id tbpt_id);

int protocol_bptree_index_first_no_key(void *p, tapioca_bptree_id tbpt_id);
int protocol_bptree_debug(void *p, tapioca_bptree_id tbpt_id,
		enum bptree_debug_option debug_opt);
#endif
