#ifndef _PROTOCOL_BTREE_H_
#define _PROTOCOL_BTREE_H_

#include "tapioca.h"
#include "tapioca_btree.h"

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
