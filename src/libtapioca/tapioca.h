#ifndef _TAPIOCA_H_
#define _TAPIOCA_H_

#ifdef __cplusplus
extern "C" {
#endif


#include "mget_result.h"

// Alex : Stuff I had to add up here to get things to compile; should be
// done somewhere else?
#include <stdint.h>
#include <stddef.h>
#include "../app/tapioca/bplustree_client.h"

typedef void tapioca_handle;


/**
 	Opens a new connection to a tapioca node with given ip address and port.

	@return a new tapioca_handle, NULL on failure.
*/
tapioca_handle* tapioca_open(const char* address, int port);


/**
	Closes tapioca_handle previously opened with tapioca_open()
*/
void tapioca_close(tapioca_handle* th);

/**
	Returns the client id of the tapioca node to which handle	is connected to.

	@param th a opened tapioca_handle
*/

int
tapioca_client_id(tapioca_handle* th);

/**
	Returns the node id of the tapioca node to which handle	is connected to.

	@param th a opened tapioca_handle
*/
int tapioca_node_id(tapioca_handle* th);


/**
	Performs a get operation on the current transaction.

	@param th a opened tapioca_handle
	@param k a pointer to the key to retrieve
	@param ksize the size of the memory area pointed by k
	@param v a pointer to a memory area in which the function can store the
	returned value
	@param vsize the size of the memory area pointed by v
	@return number of bytes written in v, 0 if the key does not exist,
	-1 if the get operation failed
*/
int tapioca_get(tapioca_handle* th, void* k, int ksize, void* v, int vsize);


/**
	Performs a put operation on the current transaction.

	@param th a opened tapioca_handle
	@param k a pointer to the key to put
	@param ksize the size of the memory area pointed by k
	@param v a pointer to the value to put
	@param vsize the size of the memory area pointed by v
	@return 1 if the operations succeeds, -1 otherwise
*/
int tapioca_put(tapioca_handle* th, void* k, int ksize, void* v, int vsize);


/**
	Commits the current transaction.
	
	@param th a opened tapioca_handle
	@return >= 0 if the transaction commits, -1 otherwise
*/
int tapioca_commit(tapioca_handle* th);


/**
	Rolls back the current transaction.
	
	@return 1 if the operations succeeds, -1 otherwise
*/
int tapioca_rollback(tapioca_handle* th);


/**
  Allows to group several get operations and submit them as one.   

  @param th a opened tapioca_handle
	@param k a pointer to the key to retrieve
	@param ksize the size of the memory area pointed by k
	@return >= 0 if the operation succeeds, -1 otherwise
*/
int tapioca_mget(tapioca_handle* th, void* k, int ksize);


/**
  Submits grouped mget operations to tapioca, and commits the current
  transaction.
  
  @param th a opened tapioca_handle
  @return returns an mget_result populated with the returned values,
  or NULL if the operation failed.
*/
mget_result* tapioca_mget_commit(tapioca_handle* th);



int tapioca_mput(tapioca_handle* th, void* k, int ksize, void* v, int vsize);
int tapioca_mput_commit(tapioca_handle* th);
int tapioca_mput_commit_retry(tapioca_handle* th, int times);

int tapioca_mget_put(tapioca_handle* th, void* k, int ksize, void* v, int vsize);
int tapioca_mget_put_commit(tapioca_handle* th);

// These are just wrappers around tapioca_mget() and tapioca_mput()
int tapioca_mget_int(tapioca_handle* th, int n, int* keys, int* values);
int tapioca_mput_int(tapioca_handle* th, int n, int* keys, int* values);

// B-tree API
int tapioca_btree_search(tapioca_handle* th, long k, long* v);
int tapioca_btree_insert(tapioca_handle* th, long k, long v);
int tapioca_btree_range(tapioca_handle* th, long min, long max);
int tapioca_btree_update(void* p, long k, long v);


/********** B+Tree operations ******************************/

tapioca_bptree_id tapioca_bptree_initialize_bpt_session_no_commit(
		tapioca_handle *th, tapioca_bptree_id tbpt_id,
		enum bptree_open_flags open_flags, uint32_t execution_id);

tapioca_bptree_id tapioca_bptree_initialize_bpt_session( tapioca_handle *th,
		tapioca_bptree_id tbpt_id, enum bptree_open_flags open_flags);

int tapioca_bptree_set_num_fields(tapioca_handle *th,
		tapioca_bptree_id tbpt_id, int16_t num_fields);

int tapioca_bptree_set_field_info(void *p,
		tapioca_bptree_id tbpt_id, int16_t field_num,
		int16_t field_sz, enum bptree_field_comparator comparator);

int tapioca_bptree_insert(tapioca_handle *th, tapioca_bptree_id tbpt_id,
		void *k, int ksize, void *v, int vsize,
		enum bptree_insert_flags insert_flags);

int tapioca_bptree_update(tapioca_handle *th, tapioca_bptree_id tbpt_id,
		void *k, int ksize, void *v, int vsize);

int tapioca_bptree_search(tapioca_handle *th, tapioca_bptree_id tbpt_id,
	void *k,int32_t ksize,void *v, int32_t *vsize);

int tapioca_bptree_index_first(tapioca_handle *th, tapioca_bptree_id tbpt_id,
	void *k,int32_t *ksize,void *v, int32_t *vsize);

int tapioca_bptree_index_next(tapioca_handle *th, tapioca_bptree_id tbpt_id,
	void *k,int32_t *ksize,void *v, int32_t *vsize);

int tapioca_bptree_index_next_mget(tapioca_handle *th,tapioca_bptree_id tbpt_id,
		bptree_mget_result **bmres, int16_t *rows);

int tapioca_bptree_index_first_no_key(tapioca_handle *th,
		tapioca_bptree_id tbpt_id);

int tapioca_bptree_debug(tapioca_handle *th, tapioca_bptree_id tbpt_id,
		enum bptree_debug_option debug_opt);

int tapioca_bptree_delete(tapioca_handle *th, tapioca_bptree_id tbpt_id,
		void *k, int ksize, void *v, int vsize);

#ifdef __cplusplus
}
#endif

#endif
