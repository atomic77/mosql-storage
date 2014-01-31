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
#include <msgpack.h>
#include <zlib.h>

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

#define BPTREE_MIN_DEGREE 5 
#define BPTREE_NODE_SIZE 2 * BPTREE_MIN_DEGREE - 1
#define BPTREE_MAX_VALUE_SIZE MAX_TRANSACTION_SIZE

#define BPTREE_META_NODE_PACKET_HEADER 0x4
#define BPTREE_NODE_PACKET_HEADER 0x5

#define TEST_TYPE_INSERT 1
#define TEST_TYPE_SEARCH 2

//#define BPTREE_DEBUG 1
//define DEFENSIVE_MODE
//#define PARANOID_MODE
//#define TRACE_MODE
#define BPTREE_NODE_COMPRESSION 0
#define COMPRESSION_LEVEL 6 // 1 fastest; 9 best compression
#undef BPTREE_MARSHALLING_TPL
#define BPTREE_MARSHALLING_MSGPACK

//#define BPTREE_TPL_NODE_FMT "Ijji#A(B)i#A(B)I#IIIc#"  // pre-UUID move
#define BPTREE_TPL_NODE_FMT "c#jji#A(B)i#A(B)c##c#c#c#c#" // array of arrays?
//#define BPTREE_TPL_NODE_FMT "c#jji#A(B)i#A(B)c##c#c#c#c#jii" // array of arrays?

// TODO There is room to optimize space usage in this struct
typedef struct bptree_node {
	uuid_t self_key;
	int16_t key_count;
	int16_t leaf;
	uuid_t parent;
	uuid_t prev_node;
	uuid_t next_node;
	char active[BPTREE_NODE_SIZE];
	int32_t key_sizes[BPTREE_NODE_SIZE];
	int32_t value_sizes[BPTREE_NODE_SIZE];
	unsigned char *keys[BPTREE_NODE_SIZE];
	unsigned char *values[BPTREE_NODE_SIZE];
	// TODO Dynamically allocate this based on whether this is a leaf or not
	uuid_t children[BPTREE_NODE_SIZE + 1];
} bptree_node;

typedef struct bptree_key_val {
	unsigned char *k;
	unsigned char *v;
	int32_t ksize; // these probably could be 16-bit but too much code to change
	int32_t vsize;
} bptree_key_val;

// See enum bptree_field_comparator in bplustree_client.h
typedef int (*bptree_comparator)(const void *a, const void *b, size_t n);
static bptree_comparator comparators[] =  {
	int8cmp,
	int16cmp,
	int32cmp,
	int64cmp,
	strncmp_wrap,
	memcmp,
	strncmp_mysql
};

#define BPTREE_TPL_META_NODE_FMT "ic#j"

typedef struct bptree_meta_node {
//	unsigned char header;
	uint32_t execution_id;
	uuid_t root_key;
	tapioca_bptree_id bpt_id;
	int32_t last_version; // we will not persist this
} bptree_meta_node;

typedef struct bptree_session {
//	uuid_t cursor_cell_id;
	int16_t cursor_pos;
	tapioca_bptree_id bpt_id;
	uint16_t eof;
	int32_t op_counter;
	uint16_t num_fields;
	bptree_field *bfield;

	// Moved from bptree_handle
	uint32_t execution_id;
	transaction *t;
	uuid_t cached_key;
	uint16_t cached_key_dirty;
	bptree_node *cursor_node;
	int tapioca_client_id;
	int insert_count;
} bptree_session;

int bptree_initialize_bpt_session_no_commit(bptree_session *bps,
		tapioca_bptree_id bpt_id, enum bptree_open_flags open_flags,
		uint32_t local_execution_id);

int bptree_initialize_bpt_session(bptree_session *bps,
		tapioca_bptree_id bpt_id, enum bptree_open_flags open_flags);
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
		void *v, int32_t vsize, enum bptree_insert_flags insert_flags);

inline int bptree_compar_keys(bptree_session *bps,
		const bptree_key_val *kv1, const bptree_key_val *kv2);
//int bptree_compar(bptree_session *bps, const void *b1, const void *b2);
int bptree_compar(bptree_session *bps, const void *k1, const void *k2,
		const void *v1, const void *v2, size_t vsize1, size_t vsize2, 
		int tot_fields);
inline void get_key_val_from_node(bptree_node *n, int i, bptree_key_val *kv);
void copy_key_val(bptree_key_val *dest, bptree_key_val *src);
bptree_key_val * copy_key_val_from_node(bptree_node *n, int i);
inline void free_key_val(bptree_key_val **kv);

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

void * marshall_bptree_meta_node(bptree_meta_node *bpm, size_t *bsize);
//bptree_meta_node * unmarshall_bptree_meta_node(const void *buf);
bptree_meta_node * unmarshall_bptree_meta_node(const void *buf,size_t sz);

void * marshall_bptree_node(bptree_node *n, size_t *bsize);
//bptree_node * unmarshall_bptree_node(const void *buf);
bptree_node * unmarshall_bptree_node(const void *buf, size_t sz, size_t *nsize);

//// *** Testing/debug functions
//int verify_bptree_order(bptree_session *bps,
//		enum bptree_order_verify mode);

int output_bptree(bptree_session *bps, int i ) ;
int bptree_sequential_read(bptree_session *bps, int binary);


int is_cell_ordered(bptree_session *bps, bptree_node* y);
int free_node(bptree_node **n);

void assert_parent_child(bptree_session *bps, bptree_node *p, bptree_node *c);
void dump_node_info(bptree_session *bps, bptree_node *n);

void print_trace (void);

void bptree_key_value_to_string(bptree_session *bps, unsigned char *k,
		unsigned char *v, int32_t ksize, int32_t vsize, char *out);
void bptree_key_value_to_string_kv(bptree_session *bps, bptree_key_val *kv,
		char *out);

bptree_node * create_new_empty_bptree_node();
bptree_node * create_new_bptree_node(bptree_session *bps);
int bptree_debug(bptree_session *bps, enum bptree_debug_option debug_opt,
		void *data);

#ifdef TRACE_MODE
int write_to_trace_file(int type,  tr_id *t, key* k, val* v, int prev_client);
#endif

#endif
