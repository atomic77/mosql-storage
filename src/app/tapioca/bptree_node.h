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

#ifndef _BPTREE_NODE_H_
#define _BPTREE_NODE_H_

#include "transaction.h"
#include "tapioca_btree.h"
#include "bplustree_util.h"

#include <uuid/uuid.h>
#include <paxos.h>

/* Note that these are the Cormen definitions, i.e. let t be the degree of the 
 * btree, so 2t is the # of children, 2t -1 is the max size of the node
 * and t -1 is the minimum size of a non-root node
 */
#define BPTREE_MIN_DEGREE 3 
#define BPTREE_NODE_MIN_SIZE (BPTREE_MIN_DEGREE -1)
#define BPTREE_NODE_SIZE (2 * BPTREE_MIN_DEGREE - 1)
#define BPTREE_NODE_MAX_CHILDREN (2 * BPTREE_MIN_DEGREE)
#define BPTREE_MAX_VALUE_SIZE MAX_TRANSACTION_SIZE

typedef struct bptree_node bptree_node;

typedef struct bptree_key_val {
	unsigned char *k;
	unsigned char *v;
	int32_t ksize; // these probably could be 16-bit but too much code to change
	int32_t vsize;
} bptree_key_val;


bptree_node * bpnode_new();
unsigned char * bpnode_get_id(bptree_node *x);
unsigned char * bpnode_get_child_id(bptree_node *x, int c_pos);
unsigned char * bpnode_get_parent_id(bptree_node *x);
unsigned char * bpnode_get_next_id(bptree_node *x);
unsigned char * bpnode_get_prev_id(bptree_node *x);
int bpnode_size(bptree_node *x);
int bpnode_is_leaf(bptree_node *x);
int bpnode_is_active(bptree_node *x, int pos);
int bpnode_is_node_active(bptree_node *x);
int bpnode_set_leaf(bptree_node *x, int leaf);
int bpnode_set_active(bptree_node *x, int pos);
int bpnode_set_inactive(bptree_node *x, int pos);
void bpnode_size_inc(bptree_node *x);
void bpnode_size_dec(bptree_node *x);

int bpnode_is_eof(bptree_node *x);

unsigned char * bpnode_get_key(bptree_node *x, int pos);
unsigned char * bpnode_get_value(bptree_node *x, int pos);
int bpnode_get_key_size(bptree_node *x, int pos);
int bpnode_get_value_size(bptree_node *x, int pos);

void bpnode_clear_parent(bptree_node *x);
void bpnode_clear_child(bptree_node *x, int c);
void bpnode_set_child(bptree_node *x, int pos,  bptree_node *c);
void bpnode_set_child_id(bptree_node *x, int pos, uuid_t id);
void bpnode_set_parent(bptree_node *c, bptree_node *p);
void bpnode_set_next(bptree_node *x, bptree_node *src);
void bpnode_set_prev(bptree_node *x, bptree_node *src);
void bpnode_set_next_id(bptree_node *x, uuid_t id);




int bpnode_is_full(bptree_node * n);
int bpnode_is_empty(bptree_node * n);
int bpnode_is_same(bptree_node *x, bptree_node *y);

void free_key_val(bptree_key_val **kv);

bptree_node * copy_node(bptree_node *n);
void copy_node_data(bptree_node *x, int j, int n);
int free_node(bptree_node **n);

/* Definitions that were in the main c file before that we want to be able
to unit test */

void shift_bptree_node_elements_right(bptree_node *x, int pos);
void shift_bptree_node_elements_left(bptree_node *x, int pos);

void shift_bptree_node_children_left(bptree_node *x, int pos);
void shift_bptree_node_children_right(bptree_node *x, int pos);
void copy_bptree_node_element(bptree_node *s, bptree_node *d,
		int s_pos, int d_pos);
void move_bptree_node_element(bptree_node *s, bptree_node *d,
		int s_pos, int d_pos);

void delete_key_from_node(bptree_node *x, int pos);

void copy_key_val_to_node(bptree_node *x, bptree_key_val *kv, int pos);
void copy_key_val(bptree_key_val *dest, bptree_key_val *src);

bptree_key_val * bpnode_get_kv(bptree_node *n, int i);
void bpnode_get_kv_ref(bptree_node *n, int i, bptree_key_val *kv);
/*@ Assume kv has pointers to alloc'd memory */
void bpnode_pop_kv(bptree_node *n, int i, bptree_key_val *kv);

void redistribute_keys(bptree_node *p, bptree_node *cl, bptree_node *cr, int i);
void concatenate_nodes(bptree_node *p, bptree_node *cl, bptree_node *cr, int i);


void redistribute_keys_rl_nonleaf(bptree_node *p, 
				  bptree_node *cl, bptree_node *cr, int i);
void redistribute_keys_lr_nonleaf(bptree_node *p, 
				  bptree_node *cl, bptree_node *cr, int i);
void redistribute_keys_rl_leaf(bptree_node *p, 
				  bptree_node *cl, bptree_node *cr, int i);
void redistribute_keys_lr_leaf(bptree_node *p, 
				  bptree_node *cl, bptree_node *cr, int i);


void clear_key_position(bptree_node *x, int i);

inline int is_bptree_node_underflowed(bptree_node *x) ;

void * marshall_bptree_node(bptree_node *n, size_t *bsize);
bptree_node * unmarshall_bptree_node(const void *buf, size_t sz, size_t *nsize);


#endif