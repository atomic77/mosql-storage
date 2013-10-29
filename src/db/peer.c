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

#include "peer.h"
#include "hashtable.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <limits.h>


static struct hashtable* peers = NULL;
static struct hashtable* recnodes = NULL;


#define HT_INIT_SIZE 128
#define HT_FUNCTIONS hash_from_key, equal_keys, NULL
#define MAX_IP_LEN 17


struct peer {
	int port;
	char address[MAX_IP_LEN];
	int node_type;
};


#define table_size 256
#define slot_size (UINT_MAX / (table_size -1))
static int node_count = 0;
static int node_table[table_size];

static void peers_init();
static void recnodes_init();
static int* create_peer_key(int id);
static int equal_keys(void* k1, void* k2);
static unsigned int hash_from_key(void* k);
static void hash_add_node(int node_id);


void peer_add(int id, char* address, int port) {
	int rv;
	struct peer* p;
	
	if (peers == NULL)
		peers_init();
	
	p = malloc(sizeof(struct peer));
	assert(p != NULL);
	
	strncpy(p->address, address, MAX_IP_LEN);
	p->port = port;
	rv = hashtable_insert(peers, create_peer_key(id), p);
	assert(rv != 0);
	
	hash_add_node(id);
}


void peer_add_cache_node(int id, char* address, int port) {
	int rv;
	struct peer* p;
	
	if (peers == NULL)
		peers_init();
	
	p = malloc(sizeof(struct peer));
	assert(p != NULL);
	strncpy(p->address, address, MAX_IP_LEN);
	p->port = port;
	rv = hashtable_insert(peers, create_peer_key(id), p);
	assert(rv != 0);
}

int peer_node_type(struct peer* p) {
	return p->node_type;
}

void peer_add_recnode(int id, char* address, int port) {
	int rv;
	struct peer* p;
	
	if (recnodes == NULL)
		recnodes_init();
	
	p = malloc(sizeof(struct peer));
	assert(p != NULL);
	strncpy(p->address, address, MAX_IP_LEN);
	p->port = port;
	rv = hashtable_insert(recnodes, create_peer_key(id), p);
	assert(rv != 0);
}


struct peer* peer_get_recnode(int id) {
	if (recnodes == NULL)
		return NULL;
	return hashtable_search(recnodes, &id);
}


struct peer* peer_get(int id) {
	if (peers == NULL)
		return NULL;
	return hashtable_search(peers, &id);
}


char* peer_address(struct peer* p) {
	return p->address;
}


int peer_port(struct peer* p) {
	return p->port;
}


int peer_count() {
	return node_count;
}


struct peer* peer_for_hash(unsigned int h) {
	return peer_get(peer_id_for_hash(h));
}


int peer_id_for_hash(unsigned int h) {
	return node_table[h / slot_size];
}


consistent_hash peer_get_default_hash() {
	return &peer_id_for_hash;
}

// returns the first i s.t. node_table[i] = node_id,
// or -1 if no such i exits
static int node_position(int node_id) {
	int i;
	for (i = 0; i < table_size; i++)
		if (node_table[i] == node_id)
			return i;
	return -1;
}


static void hash_add_node(int node_id) {
	int i = 0, j, slots_to_take;
	
	if (node_count == 0) {
		memset(node_table, node_id, table_size*sizeof(int));
		node_count = 1;
		return;
	}

	slots_to_take = table_size / (node_count+1);

	// at each iteration take one slot
	// from a node (round robin like)
	while (slots_to_take > 0) {
		j = i++ % node_count; 
		j = node_position(j);
		if (j < 0) continue;
		node_table[j] = node_id;
		slots_to_take--;
	}
	node_count++;
}


static void peers_init() {
	peers = create_hashtable(HT_INIT_SIZE, HT_FUNCTIONS);
	assert(peers != NULL);
	memset(node_table, 0, table_size*sizeof(int));
}


static void recnodes_init() {
	recnodes = create_hashtable(HT_INIT_SIZE, HT_FUNCTIONS);
	assert(recnodes != NULL);
}


static int* create_peer_key(int id) {
	int* i = malloc(sizeof(int));
	assert(i != NULL);
	*i = id;
	return i;
}


static int equal_keys(void* k1, void* k2) {
	int* id1 = (int*)k1;
	int* id2 = (int*)k2;
	return *id1 == *id2;
}


static unsigned int hash_from_key(void* k) {
    int* id = (int*)k;
	return (unsigned int)*id;
}
