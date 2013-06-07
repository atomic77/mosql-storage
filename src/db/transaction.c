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

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "sm.h"
#include "remote.h"
#include "hash.h"
#include "transaction.h"
#include "hashtable_itr.h"

#include <libpaxos.h>

static void add_to_set(struct hashtable* h, key* k, val* v);
static void remote_get_cb(key* k, val* v, void* arg);
static int key_equal(void* k1, void* k2);
static unsigned int hash_from_key(void* k);
static void free_key(void* k);

#define hashtable_init 64, hash_from_key, key_equal, free_key


transaction* transaction_new() {
    transaction* t;
    
    t = DB_MALLOC(sizeof(transaction));
    t->st = -1;
	t->seqn = 0;
	t->remote_count = 0;
    t->rs = create_hashtable(hashtable_init);
    t->ws = create_hashtable(hashtable_init);
	t->get_cb = NULL;
	t->cb_arg = NULL;
	t->cb_val = NULL;
	t->id.client_id = 44444;
	t->never_set = 55555;
	
    return t;
}


void transaction_clear(transaction* t) {
    t->st = -1;
	t->remote_count = 0;
	if (hashtable_count(t->rs) > 0) {
    	hashtable_destroy(t->rs, 1);
		t->rs = create_hashtable(hashtable_init);
	}
	if (hashtable_count(t->ws) > 0) {
    	hashtable_destroy(t->ws, 1);
		t->ws = create_hashtable(hashtable_init);
	}
}


void transaction_clear_writeset(transaction* t) {
	if (hashtable_count(t->ws) > 0) {
		hashtable_destroy(t->ws, 1);
    	t->ws = create_hashtable(hashtable_init);
	}
}

void transaction_clear_readset(transaction* t) {
	if (hashtable_count(t->rs) > 0) {
    	hashtable_destroy(t->rs, 1);
		t->rs = create_hashtable(hashtable_init);
	}
}


void transaction_destroy(transaction* t) {
    hashtable_destroy(t->rs, 1);
    hashtable_destroy(t->ws, 1);
    DB_FREE(t);
}


val* transaction_get(transaction* t, key* k) {
    val* v;
    flat_key_val* kv;
    
    // If read set is empty, mark current ST
    if ((t->st == -1) && (hashtable_count(t->rs) == 0))
		t->st = cproxy_current_st();
	
	// Lookup write set
	kv = hashtable_search(t->ws, k);
	if (kv != NULL) {
		v = val_new(&kv->data[kv->ksize], kv->vsize);
		// if (v->size > 0)
			add_to_set(t->rs, k, v);
		return v;
	}

    // Lookup read set
	kv = hashtable_search(t->rs, k);
	if (kv != NULL) {
		v = val_new(&kv->data[kv->ksize], kv->vsize);
		return v;
	}
	
    // Lookup storage
   	if ((v = sm_get(k, t->st)) != NULL) {
       	add_to_set(t->rs, k, v);
		return v;
	}
	
	// Issue a remote get and return NULL
	remote_get(k, t->st, remote_get_cb, t);
	return NULL;
}


int transaction_put(transaction* t, key* k, val* v) {
    // If write set is empty, mark current ST
    if ((t->st == -1) && (hashtable_count(t->ws) == 0))
        t->st = cproxy_current_st();
    
	add_to_set(t->ws, k, v);
	return 0;
}


int transaction_commit(transaction* t, int id, cproxy_commit_cb cb) {
	int size;
	static char buffer[MAX_TRANSACTION_SIZE];
	
    t->id.client_id = id;
	t->id.seqnumber = t->seqn;
	t->id.node_id = NodeID;
	t->seqn++;
	
	size = transaction_serialize(t, (tr_submit_msg*)buffer, MAX_TRANSACTION_SIZE);
	
	if (size < 0) {
		printf("transaction_serialize failed\n");
		return -1;
	}
	
    if (cproxy_submit(buffer, size, cb) < 0) {
		printf("cproxy_submit failed");
        return -1;
	}
	
    return 0;
}


int transaction_read_only(transaction* t) {
    return (hashtable_count(t->ws) == 0);
}


int transaction_serialize(transaction* t, tr_submit_msg* msg, int max_size) {
    int size = 0;
    int hsize = 0;
	key* k;
    flat_key_val* kv;
    flat_key_hash* kh;
	struct hashtable_itr* itr;
	
	msg->type = TRANSACTION_SUBMIT;
    msg->id.client_id = t->id.client_id;
    msg->id.seqnumber = t->id.seqnumber;
    msg->id.node_id = NodeID;
    msg->start = t->st;
    msg->readset_count = 0;
    msg->writeset_count = 0;
    
    // copy readset hashes
	if (hashtable_count(t->rs) > 0) {
		itr = hashtable_iterator(t->rs);
		do {
			if ((size + (int)sizeof(flat_key_hash)) > max_size) {
				return -1;
			}
				
			k = hashtable_iterator_key(itr);
			kh = (flat_key_hash*)&msg->data[size];
			
			kh->hash[0] = hashtable_iterator_hash(itr);
			kh->hash[1] = djb2_hash(k->data, k->size);
			
			size += sizeof(flat_key_hash);
			msg->readset_count++;
		} while (hashtable_iterator_advance(itr));
		free(itr);
	}

    
    // copy writeset hashes
	if (hashtable_count(t->ws) > 0) {
		itr = hashtable_iterator(t->ws);
		do {
			if ((size + (int)sizeof(flat_key_hash)) > max_size) {
				return -1;
			}
			
			k = hashtable_iterator_key(itr);
			kh = (flat_key_hash*)&msg->data[size];
			
			kh->hash[0] = hashtable_iterator_hash(itr);
			kh->hash[1] = djb2_hash(k->data, k->size);
			
			size += sizeof(flat_key_hash);
			msg->writeset_count++;
		} while (hashtable_iterator_advance(itr));
		free(itr);
		hsize = size;
		
		itr = hashtable_iterator(t->ws);
		do {
			kv = hashtable_iterator_value(itr);
			if ((size + (int)FLAT_KEY_VAL_SIZE(kv)) > max_size) {
				return -1;
			}
			
			memcpy(&msg->data[size], kv, FLAT_KEY_VAL_SIZE(kv));
			size += (int)FLAT_KEY_VAL_SIZE(kv);
		} while (hashtable_iterator_advance(itr));
		free(itr);
	}
    msg->writeset_size = (size - hsize);
    return TR_SUBMIT_MSG_SIZE(msg);
}


void transaction_set_get_cb(transaction* t, transaction_cb cb, void* arg) {
	t->get_cb = cb;
	t->cb_arg = arg;
}


int transaction_remote_count(transaction* t) {
	return t->remote_count;
}


static void remote_get_cb(key* k, val* v, void* arg) {
	transaction* t;
	t = (transaction*)arg;
	t->remote_count++;
	add_to_set(t->rs, k, v);
	if (t->get_cb != NULL)
		t->get_cb(k, v, t->cb_arg);
}


static void add_to_set(struct hashtable* h, key* k, val* v) {
	int rv;
	key* new_key;
	flat_key_val* new_value;
	
	new_value = hashtable_search(h, k);
	if (new_value == NULL) {
		new_key = key_new(k->data, k->size);
		new_value = malloc(sizeof(flat_key_val) + k->size + v->size);
		assert(new_value != NULL);
		rv = hashtable_insert(h, new_key, new_value);
		assert(rv != 0);
	} else {
		if ((k->size + v->size) != (new_value->ksize + new_value->vsize)) {
			new_key = key_new(k->data, k->size);
			new_value = hashtable_remove(h, k);
			free(new_value);
			new_value = malloc(sizeof(flat_key_val) + k->size + v->size);
			assert(new_value != NULL);
			rv = hashtable_insert(h, new_key, new_value);
			assert(rv != 0);
		}
	}
	
	new_value->ksize = k->size;
	new_value->vsize = v->size;
	memcpy(new_value->data, k->data, k->size);
	memcpy(&new_value->data[k->size], v->data, v->size);
}


static int key_equal(void* k1, void* k2) {
	key* a = (key*)k1;
	key* b = (key*)k2;
	
	if (a->size != b->size)
		return 0;
	
	if (memcmp(a->data, b->data, a->size) == 0)
		return 1;
	
	return 0;
}


static unsigned int hash_from_key(void* k) {
	key* x = (key*)k;
	if (x->size == sizeof(int))
		return *((unsigned int*)x->data);
	return joat_hash(x->data, x->size);
}


static void free_key(void* k) {
	key* x;
	x = (key*)k;
	key_free(x);
}
