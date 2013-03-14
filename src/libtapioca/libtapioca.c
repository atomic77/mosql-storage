#include "tapioca.h"
#include "protocol.h"
#include <stdlib.h>


tapioca_handle* 
tapioca_open(const char* address, int port) {
	return protocol_open(address, port);
}


int
tapioca_client_id(tapioca_handle* th) {
	return protocol_client_id(th);
}

int
tapioca_node_id(tapioca_handle* th) {
	return protocol_node_id(th);
}


void 
tapioca_close(tapioca_handle* th) {
	protocol_close(th);
}


int
tapioca_get(tapioca_handle* th, void* k, int ksize, void* v, int vsize) {
	return protocol_get(th, k, ksize, v, vsize);
}


int
tapioca_put(tapioca_handle* th, void* k, int ksize, void* v, int vsize) {
	return protocol_put(th, k, ksize, v, vsize);
}


int
tapioca_commit(tapioca_handle* th) {
	return protocol_commit(th);
}


int
tapioca_rollback(tapioca_handle* th) {
	return protocol_rollback(th);
}


int
tapioca_mget_int(tapioca_handle* th, int n, int* keys, int* values) {
	return protocol_mget_int(th, n, keys, values);
}


int
tapioca_mput_int(tapioca_handle* th, int n, int* keys, int* values) {
	return protocol_mput_int(th, n, keys, values);
}


int 
tapioca_mget(tapioca_handle* th, void* k, int ksize) {
	return protocol_mget(th, k, ksize);
}


mget_result*
tapioca_mget_commit(tapioca_handle* th) {
	return protocol_mget_commit(th);
}


int
tapioca_mput(tapioca_handle* th, void* k, int ksize, void* v, int vsize) {
	return protocol_mput(th, k, ksize, v, vsize);
}


int
tapioca_mput_commit(tapioca_handle* th) {
	return protocol_mput_commit(th);
}


int
tapioca_mput_commit_retry(tapioca_handle* th, int times) {
	return protocol_mput_commit_retry(th, times);
}


int
tapioca_mget_put(tapioca_handle* th, void* k, int ksize, void* v, int vsize) {
	return protocol_mget_put(th, k, ksize, v, vsize);
}


int
tapioca_mget_put_commit(tapioca_handle* th) {
	return protocol_mget_put_commit(th);
}


int 
tapioca_btree_search(tapioca_handle* th, long k, long* v) {
	return protocol_btree_search(th, k, v);
}


int
tapioca_btree_insert(tapioca_handle* th, long k, long v) {
	return protocol_btree_insert(th, k, v);
}

int
tapioca_btree_range(tapioca_handle* th, long min, long max) {
	return protocol_btree_range(th, min, max);
}

int
tapioca_btree_update(tapioca_handle* th, long k, long v) {
    return protocol_btree_update(th, k, v);
}

/********** B+Tree operations ******************************/

tapioca_bptree_id
tapioca_bptree_initialize_bpt_session_no_commit(tapioca_handle *th,
	tapioca_bptree_id bpt_id, enum bptree_open_flags open_flags, uint32_t execution_id)
{
	int rv = protocol_bptree_initialize_bpt_session_no_commit(th,
			   bpt_id, open_flags, execution_id);

	if (rv == 1) return bpt_id;
	return -1;
}

tapioca_bptree_id
tapioca_bptree_initialize_bpt_session(tapioca_handle *th,
		tapioca_bptree_id bpt_id, enum bptree_open_flags open_flags)
{
	int rv = protocol_bptree_initialize_bpt_session(th, bpt_id, open_flags);
	if (rv == 1) return bpt_id;
	return -1;
}

int tapioca_bptree_set_num_fields(
		tapioca_handle *th,tapioca_bptree_id tbpt_id,  int16_t num_fields)
{
	return protocol_bptree_set_num_fields(th,tbpt_id, num_fields);
}

int tapioca_bptree_set_field_info(tapioca_handle *th,
		tapioca_bptree_id tbpt_id, int16_t field_num,
		int16_t field_sz, enum bptree_field_comparator comparator)
{
	return
		protocol_bptree_set_field_info(th,tbpt_id,field_num,field_sz,comparator);
}


int tapioca_bptree_insert(tapioca_handle *th, tapioca_bptree_id tbpt_id,
	void *k,int32_t ksize,void *v, int32_t vsize, enum bptree_insert_flags insert_flags)
{
	return protocol_bptree_insert(th,tbpt_id,k,ksize,v,vsize,insert_flags);

}

int tapioca_bptree_update(tapioca_handle *th, tapioca_bptree_id tbpt_id,
	void *k,int32_t ksize,void *v, int32_t vsize)
{
	return protocol_bptree_update(th,tbpt_id,k,ksize,v,vsize);
}

int tapioca_bptree_search(tapioca_handle *th, tapioca_bptree_id tbpt_id,
	void *k,int32_t ksize,void *v, int32_t *vsize)
{
	return protocol_bptree_search(th,tbpt_id,k,ksize,v,vsize);
}

int tapioca_bptree_index_first(tapioca_handle *th, tapioca_bptree_id tbpt_id,
	void *k,int32_t *ksize,void *v, int32_t *vsize)
{
	return protocol_bptree_index_first(th,tbpt_id,k,ksize,v,vsize);
}

int tapioca_bptree_index_next(tapioca_handle *th, tapioca_bptree_id tbpt_id,
	void *k,int32_t *ksize,void *v, int32_t *vsize)
{
	return protocol_bptree_index_next(th,tbpt_id,k,ksize,v,vsize);
}

int tapioca_bptree_index_next_mget(tapioca_handle *th,tapioca_bptree_id tbpt_id,
		bptree_mget_result **bmres, int16_t *rows)
{
	return protocol_bptree_index_next_mget(th,tbpt_id,bmres, rows);
}

int tapioca_bptree_index_first_no_key(tapioca_handle *th,
		tapioca_bptree_id tbpt_id)
{
	return protocol_bptree_index_first_no_key(th, tbpt_id);
}

int tapioca_bptree_debug(tapioca_handle *th, tapioca_bptree_id tbpt_id,
		enum bptree_debug_option debug_opt)
{
	return protocol_bptree_debug(th, tbpt_id, debug_opt);
}

int tapioca_bptree_delete(tapioca_handle *th, tapioca_bptree_id tbpt_id,
	void *k,int32_t ksize,void *v, int32_t vsize)
{
	return protocol_bptree_delete(th,tbpt_id,k,ksize,v,vsize);
}

