#ifndef _BTREE_H_
#define _BTREE_H_

#include "transaction.h"

#define BTREE_MIN_DEGREE 12

typedef struct btree_t {
	long root_key;
} btree;


typedef struct btree_node {
	long self_key;
	unsigned short key_count;
	unsigned short leaf;
	long keys[2 * BTREE_MIN_DEGREE - 1];
	long values[2 * BTREE_MIN_DEGREE - 1];
	long children[2 * BTREE_MIN_DEGREE];
} btree_node;


int btree_insert(transaction* t, long k, long v);
int btree_update(transaction* t, long k, long v);
int btree_search(transaction* t, long k, long* v);
int btree_range(transaction* t, long min, long max, long* range, long* range_count);

#endif
