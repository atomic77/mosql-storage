/*
    Copyright (C) 2013 University of Lugano

	This file is part of the MoSQL storage system. 

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

//#ifndef _BTREE_H_
//#define _BTREE_H_
#ifndef _BPLUSTREE_H_
#define _BPLUSTREE_H_

#include "transaction.h"
#include "tapioca_btree.h"
#include "bptree_node.h"
#include <zlib.h>
#include <msgpack.h>

#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <limits.h>
#include <stdint.h>
#include <math.h>
#include <unistd.h>
#include <uuid/uuid.h>
#include <execinfo.h>
#include <paxos.h>

#define TEST_TYPE_INSERT 1
#define TEST_TYPE_SEARCH 2

//#define BPTREE_DEBUG 1
#define DEFENSIVE_MODE
//#define TRACE_MODE
#define BPTREE_NODE_COMPRESSION 0
#define COMPRESSION_LEVEL 6 // 1 fastest; 9 best compression
#undef BPTREE_MARSHALLING_TPL
#define BPTREE_MARSHALLING_MSGPACK

#define BPTREE_META_NODE_PACKET_HEADER 0x4
#define BPTREE_NODE_PACKET_HEADER 0x5


//#define BPTREE_TPL_NODE_FMT "Ijji#A(B)i#A(B)I#IIIc#"  // pre-UUID move
#define BPTREE_TPL_NODE_FMT "c#jji#A(B)i#A(B)c##c#c#c#c#" // array of arrays?
//#define BPTREE_TPL_NODE_FMT "c#jji#A(B)i#A(B)c##c#c#c#c#jii" // array of arrays?

// See enum bptree_field_comparator in bplustree_client.h
typedef int (*bptree_comparator)(const void *a, const void *b, size_t n);
static bptree_comparator comparators[] =  {
	int8cmp,
	int16cmp,
	int32cmp,
	int64cmp,
	strncmp_wrap,
	memcmp,
	strncmp_mysql,
	strncmp_mysql_var
};

#define BPTREE_TPL_META_NODE_FMT "ic#j"

typedef struct bptree_session {
	int16_t cursor_pos;
	tapioca_bptree_id bpt_id;
	uint16_t eof;
	int32_t op_counter;
	uint16_t num_fields;
	uint16_t idx_len;
	bptree_field *bfield;

	// Moved from bptree_handle
	uint32_t execution_id;
	transaction *t;
	uint16_t cached_key_dirty;
	bptree_node *cursor_node;
	int tapioca_client_id;
	int insert_count;
	enum bptree_insert_flags insert_flags;
} bptree_session;

typedef struct bptree_meta_node {
	uint32_t execution_id;
	uuid_t root_key;
	tapioca_bptree_id bpt_id;
	int32_t last_version; // we will not persist this
} bptree_meta_node;



int bptree_initialize_bpt_session_no_commit(bptree_session *bps,
		tapioca_bptree_id bpt_id, enum bptree_open_flags open_flags,
		enum bptree_insert_flags insert_flags,
		uint32_t local_execution_id);

int bptree_initialize_bpt_session(bptree_session *bps,
		tapioca_bptree_id bpt_id, enum bptree_open_flags open_flags,
		enum bptree_insert_flags insert_flags);

int bptree_set_active_bpt_id(bptree_session *bps, tapioca_bptree_id bpt_id);

int bptree_set_num_fields(bptree_session *bps, int16_t num_fields) ;
int bptree_set_field_info(bptree_session *bps, int16_t field_num,
		int16_t field_sz, enum bptree_field_comparator field_type,
		int(*compar)(const void *, const void *, size_t n));


int bptree_close(bptree_session *bps);
int bptree_commit(bptree_session *bps);
int bptree_rollback(bptree_session *bps);

int bptree_update(bptree_session *bps,  void *k, int32_t ksize,
		void *v, int32_t vsize);

int bptree_insert(bptree_session *bps, void *k, int32_t ksize,
		void *v, int32_t vsize);

int bptree_compar_keys(bptree_session *bps,
		const bptree_key_val *kv1, const bptree_key_val *kv2);
//int bptree_compar(bptree_session *bps, const void *b1, const void *b2);
int bptree_compar(bptree_session *bps, const void *k1, const void *k2,
		const void *v1, const void *v2, size_t vsize1, size_t vsize2, 
		int tot_fields);
inline void bpnode_get_kv_ref(bptree_node *n, int i, bptree_key_val *kv);
void copy_key_val(bptree_key_val *dest, bptree_key_val *src);
bptree_key_val * bpnode_get_kv(bptree_node *n, int i);

void free_meta_node(bptree_meta_node **m);

void * marshall_bptree_meta_node(bptree_meta_node *bpm, size_t *bsize);
bptree_meta_node * unmarshall_bptree_meta_node(const void *buf,size_t sz);

/**
 * Searches b+tree for element; if found, set internal cursor to enable
 * calls to index_next/prev
 */
int bptree_search(bptree_session *bps, void *k,
		int32_t ksize, void *v, int32_t *vsize);
int bptree_index_first(bptree_session *bps, void *k,
		int32_t *ksize, void *v, int32_t *vsize);
int bptree_index_first_no_key(bptree_session *bps);
int bptree_index_next(bptree_session *bps, void *k,
		int32_t *ksize, void *v, int *vsize);
int bptree_index_next_mget(bptree_session *bps,
		bptree_mget_result **bmres, int16_t *rows, int *buf_sz);
int bptree_delete(bptree_session *bps, void *k,
		int32_t ksize, void *v, int32_t vsize);

// Quasi-external facing methods
bptree_node * read_node(bptree_session *bps,
		uuid_t node_key, int *rv);

int write_node(bptree_session *bps, bptree_node* n);
bptree_meta_node * read_meta_node(bptree_session *bps, int *rv);
int write_meta_node(bptree_session *bps,
		bptree_meta_node* bpm, bptree_node *root) ;
int bptree_read_root(bptree_session *bps, bptree_meta_node **bpm,
		bptree_node **root);

void bptree_split_child(bptree_session *bps, bptree_node* p, int i, 
		       bptree_node* cl, bptree_node *cr);

//// *** Testing/debug functions

int output_bptree(bptree_session *bps, int i ) ;
int bptree_sequential_read(bptree_session *bps, int binary);


int is_node_ordered(bptree_session *bps, bptree_node* y);
int free_node(bptree_node **n);

void assert_parent_child(bptree_session *bps, bptree_node *p, bptree_node *c);
void dump_node_info(bptree_session *bps, bptree_node *n);
void dump_node_info_fp(bptree_session *bps, bptree_node *n, FILE *fp);

void print_trace (void);

void bptree_key_value_to_string(bptree_session *bps, unsigned char *k,
		unsigned char *v, int32_t ksize, int32_t vsize, char *out);
void bptree_key_value_to_string_kv(bptree_session *bps, bptree_key_val *kv,
		char *out);

int bptree_debug(bptree_session *bps, enum bptree_debug_option debug_opt,
		void *data);

// Node assertion functions

int is_node_ordered(bptree_session *bps, bptree_node* y);
int are_split_cells_valid(bptree_session *bps, bptree_node* x, int i, bptree_node *y, bptree_node *n);
int is_node_sane(bptree_node *n);
int is_correct_node(bptree_node *n, uuid_t node_key);
void bptree_free_session(bptree_session **bps);

int bptree_compar_to_node(bptree_session *bps,
	bptree_node *x, const bptree_key_val *kv, int pos);

int is_valid_traversal(bptree_session *bps, bptree_node *x,
		bptree_node *n,int i);

/*@ For *kv, returns whether it was found; k_pos is where it is/would be
 * in the node, and c_pos is what child position we would find the key
 * in a further traversal*/
int find_position_in_node(bptree_session *bps, bptree_node *x,
		bptree_key_val *kv, int *k_pos, int *c_pos);

void bptree_split_child_leaf(bptree_session *bps, bptree_node* p, int i, 
		       bptree_node* cl, bptree_node *cr);
void bptree_split_child_nonleaf(bptree_session *bps, bptree_node* p, int i, 
		       bptree_node* cl, bptree_node *cr);

#ifdef TRACE_MODE
int write_to_trace_file(int type,  tr_id *t, key* k, val* v, int prev_client);
#endif

#endif
