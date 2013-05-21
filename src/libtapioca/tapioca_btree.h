#ifndef _TAPIOCA_BTREE_H_
#define _TAPIOCA_BTREE_H_

#include <stdlib.h>
#include <stdint.h>
#include "tapioca.h"

#define BPTREE_META_NODE_KEY 0
#define BPTREE_MAX_NUMBER_FIELDS 9

/* B+Tree error codes */
#define BPTREE_ERR_DUPLICATE_KEY_INSERTED 3

#define BPTREE_OP_KEY_NOT_FOUND 0
#define BPTREE_OP_TAPIOCA_NOT_READY 2
#define BPTREE_OP_NODE_FOUND 4
#define BPTREE_OP_KEY_FOUND 8
#define BPTREE_OP_EOF 16
#define BPTREE_OP_KEY_DELETED 32
// Catch-all for 'good' things we haven't bothered to define yet
#define BPTREE_OP_SUCCESS 1

#define BPTREE_OP_FAIL -1
#define BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT -2
#define BPTREE_OP_TREE_ERROR -4
#define BPTREE_OP_METADATA_ERROR -8
#define BPTREE_OP_INVALID_INPUT -16
#define BPTREE_OP_RETRY_NEEDED -32

//#define BPTREE_VALUE_SIZE 64000

enum bptree_debug_option
{
	/* Recurse to first element; then read tree using next() */
	BPTREE_DEBUG_VERIFY_SEQUENTIALLY=0,
	BPTREE_DEBUG_VERIFY_RECURSIVELY=1,  /* Traverse tree recursively */
	BPTREE_DEBUG_DUMP_SEQUENTIALLY=2, /* _next() based dump of tree */
	BPTREE_DEBUG_DUMP_RECURSIVELY=3, /* recursive traversal of tree */
	BPTREE_DEBUG_INDEX_RECURSIVE_SCAN=4, /* multi-node compatible recurs. scan*/
	BPTREE_DEBUG_DUMP_GRAPHVIZ=5 /* Dump graphviz-viewable tree*/
};

enum bptree_field_comparator {
	BPTREE_FIELD_COMP_INT_8=0,
	BPTREE_FIELD_COMP_INT_16=1,
	BPTREE_FIELD_COMP_INT_32=2,
	BPTREE_FIELD_COMP_INT_64=3,
	BPTREE_FIELD_COMP_STRNCMP=4,
	BPTREE_FIELD_COMP_MEMCMP=5,
	BPTREE_FIELD_COMP_MYSQL_STRNCMP=6
};


// The current definition of this structure requires that we have
// only fixed-length index fields
typedef struct bptree_field {
	int16_t f_sz;
	int(*compar)(const void *, const void *, size_t n);
	enum bptree_field_comparator field_type;
} bptree_field;

// Now a synonym for a bpt_id
typedef int16_t tapioca_bptree_id ;

// A linked-list of results
typedef struct bptree_mget_result {
//	int16_t key_count;
	int32_t ksize;
	unsigned char *k;
	int32_t vsize;
	unsigned char *v;
	struct bptree_mget_result *next;
} bptree_mget_result;

enum bptree_insert_flags
{
	BPTREE_INSERT_ALLOW_DUPES, /* Permit duplicate key values in b+tree */
	BPTREE_INSERT_UNIQUE_KEY /* A duplicate key insert is treated as an error
								condition and current tx rolled back! */
};

enum bptree_open_flags
{
	BPTREE_OPEN_CREATE_IF_NOT_EXISTS,
	BPTREE_OPEN_OVERWRITE,
	BPTREE_OPEN_ONLY /* i.e. throw error if bpt_id doesn't exist */
};
void bptree_mget_result_free(bptree_mget_result **bmres);

/// **** Comparison functions

// TODO This is quick and convenient now, but we need to find a better way to
// implement these various comparison functions

// inline
int int8cmp (const void *i1, const void *i2, size_t v_ignored);
int int16cmp (const void *i1, const void *i2, size_t v_ignored);
int int32cmp (const void *i1, const void *i2, size_t v_ignored);
int int64cmp (const void *i1, const void *i2, size_t v_ignored);
int strncmp_mysql(const void *i1, const void *i2, size_t sz);



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


#endif
