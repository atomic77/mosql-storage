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

#include <signal.h>
#include <errno.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "sm.h"
#include "socket_util.h"
#include "hash.h"
#include "transaction.h"
#include "hashtable.h"
#include "config_reader.h"
#include "util.h"

#include "lp_learner.h"
#include "lp_timers.h"
#include "lp_utils.h"

static int batch_count = 6;
#define tx_per_batch 8
#define tsize 1
#define value_size 1024
#define nitems 5000000
#define send_buffer_size 9000

typedef struct batch_t {
	int id;
	int tx_count;
	struct event timeout_ev;
	int buffer_offset;
	char buffer[send_buffer_size];
} batch;


static int ST = 0;
static struct hashtable* batches;


static int sent_count = 0;
static int recv_count = 0;
static int timeout_count = 0;
static struct timeval timeout = {0, 500000};


static int send_sock;


static void on_timeout(int fd, short event, void* arg);
static void send_batch();
static void do_one_batch(int tx);
static int key_equal(void* k1, void* k2);
static unsigned int hash_from_key(void* k);


static int next_batch_id() {
	static int ctr = 1;
	return ctr++;
}


static void do_get(transaction* t, int i) {
    key k;
    val* v;

    k.size = sizeof(int);
    k.data = &i;
    v = transaction_get(t, &k);
    if(v != NULL)
      val_free(v);
}


static void do_put(transaction* t, int i) {
    key k;
    val v;
   	static char value[value_size];

    k.size = sizeof(int);
    k.data = &i;
    v.size = value_size;
    v.data = value;
    v.version = 0;
    
    transaction_put(t, &k, &v);
}


static void generate_tx(transaction* t, int id) {
	int i, r;
	
	for (i = 0; i < tsize; i++) {
		r = random_between(0, nitems);
		do_get(t, r);
		do_put(t, r);
	}
	
	t->st = ST;
	t->id.client_id = id;
	t->id.node_id = NodeID;
}


static void batch_tx(batch* b, transaction* t) {
	int size, max_size;
	tr_submit_msg* tmsg;
	
	tmsg = (tr_submit_msg*)&b->buffer[b->buffer_offset];
	max_size = (send_buffer_size - b->buffer_offset) - sizeof(tr_submit_msg);
 	size = transaction_serialize(t, tmsg, max_size);
 	assert(size > 0);
	b->buffer_offset += size;
}


static void on_timeout(int fd, short event, void* arg) {
	batch* b;
	
	b = (batch*)arg;
	evtimer_del(&b->timeout_ev);
	send_batch(b);
	timeout_count++;
}


static void send_batch(batch* b) {
	int rv;

	rv = send(send_sock, b->buffer, b->buffer_offset, 0);
	assert(rv != 0);
		
	evtimer_set(&b->timeout_ev, on_timeout, b);
	evtimer_add(&b->timeout_ev, &timeout);
	sent_count += b->tx_count;
}


static void save_batch(batch* b) {
	int rv;
	int* k;
	
	k = malloc(sizeof(int));
	*k = b->id;
	
	rv = hashtable_insert(batches, k, b);
	assert(rv != 0);
}


static void do_one_batch(int tx) {
    int i;
	batch* b;
    transaction* t;

	b = malloc(sizeof(batch));
	b->buffer_offset = 0;
	b->id = next_batch_id();
	b->tx_count = tx;
	
	t = transaction_new();
	
	for (i = 0; i < tx; i++) {
		generate_tx(t, b->id);
	    batch_tx(b, t);
		transaction_clear(t);
	}
	
	save_batch(b);
	send_batch(b);
	transaction_destroy(t);
}


static int remove_batch(int bid) {
	batch* b;
	int tx_count = 0;
	
	b = hashtable_remove(batches, &bid);
	if (b != NULL) {
		tx_count = b->tx_count;
		evtimer_del(&b->timeout_ev);
		free(b);
	}
	
	return tx_count;
}


static void on_deliver(void* v, size_t s) {
	int i, tx_count = 0;
    tr_id* id;
    tr_deliver_msg* dmsg;

	dmsg = (tr_deliver_msg*)v;
	assert(dmsg->ST > ST);
	
	ST = dmsg->ST;
	
	for (i = 0; i < (dmsg->aborted_count + dmsg->committed_count); i++) {
		id = &((tr_id*)dmsg->data)[i];
		if (id->node_id == NodeID) {
			tx_count += remove_batch(id->client_id);
		}
	}
	
	recv_count += tx_count;
	
	while (tx_count > 0) {
		if (tx_count > tx_per_batch) {
			do_one_batch(tx_per_batch);
			tx_count -= tx_per_batch;
		} else {
			do_one_batch(tx_count);
			tx_count = 0;
		}
	}
}


static void start(int fd, short event, void* arg) {
	int i;
	struct event* ev;
	
	for (i = 0; i < batch_count; i++)
		do_one_batch(tx_per_batch);
	
	ev = (struct event*)arg;
	evtimer_del(ev);
	free(ev);
}


static void stop(int sig) {
    printf("Final ST: %d\n", ST);
	printf("Tx sent: %d\n", sent_count);
	printf("Tx recv: %d\n", recv_count);
	printf("Tx timeout: %d\n", timeout_count);
	learner_print_eventcounters();
    exit(0);
}


static void sm_populate() {
    int i;
    key k;
    val v;
    
    k.size = sizeof(int);
    v.size = sizeof(int);
    v.version = 0;
    
    for (i = 0; i < nitems; i++) {
        k.data = &i;
        v.data = &i;
        sm_put(&k, &v);
   	}
}


static void init() {
	struct event* ev;
	struct timeval tv = {1, 0};
	
	ev = malloc(sizeof(struct event));
	evtimer_set(ev, start, ev);
	evtimer_add(ev, &tv);
	
	send_sock = udp_socket_connect(LeaderIP, LeaderPort);
}


static void print_settings() {
	printf("tx_per_batch %d\n", tx_per_batch);
	printf("batch_count %d\n", batch_count);
	printf("tsize %d\n", tsize);
	printf("value_size %d\n", value_size);
	printf("nitems %d\n", nitems);
	printf("send_buffer_size %d\n", send_buffer_size);
}


int main(int argc, char const *argv[]) {
	const char * ring_paxos_config;
	
	if (argc != 4 && argc != 5) {
        printf("Usage: %s <node id> <dsmdb config> <paxos config> [batch count]\n", argv[0]);
        exit(1);
    }

	if (argc == 5) {
		batch_count = atoi(argv[4]);
	}

	if (batch_count == 0)
		exit(1);
	
	print_settings();
	
	signal(SIGINT, stop);
	
	NodeID = atoi(argv[1]);
	load_config_file(argv[2]);
	ring_paxos_config = argv[3];
	
	event_init();
	sm_init();
	sm_populate();
	batches = create_hashtable(64, hash_from_key, key_equal, NULL);
	
	learner_init(ring_paxos_config, init, on_deliver);
	event_dispatch();
	
	return 0;
}


static int key_equal(void* k1, void* k2) {
	int* a = (int*)k1;
	int* b = (int*)k2;
	return (*a == *b);
}


static unsigned int hash_from_key(void* k) {
	return joat_hash(k, sizeof(int));
}
