#include "cproxy.h"
#include "sm.h"
#include "dsmDB_priv.h"

#include "event.h"
#include "peer.h"
#include "socket_util.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

#include <event2/event.h>
#include "libpaxos.h"


#define bsize MAX_TRANSACTION_SIZE
#define BATCHING 0


typedef struct batch_t {
	int count;
	char buffer[bsize];
	size_t buffer_offset;
	struct event timeout_ev;
} batch;

static batch tx_batch;
static struct timeval max_batch_time = {0, 1000};
static cproxy_commit_cb commit_cb;

static int ST;
static int cert_sock;
static int submitted_batch = 0;
static int batch_timeout = 0;
static int submitted_tx = 0;
static int delivered_tx = 0;
static int delivered_tx_clients = 0;
static int abort_count = 0;
static int commit_count = 0;


static void on_init();
static void on_deliver(void* value, size_t size, iid_t iid,
		ballot_t ballot, int prop_id, void *arg) ;
static void on_batch_timeout(int fd, short event, void* arg);
static void init_batch(batch* b);
static void send_batch(batch* b);
static void add_to_batch(batch* b, char* v, size_t s);
static void print_stats();


int cproxy_init(const char* paxos_config, struct event_base *base) {
	struct learner *l;
	ST = 0;
	cert_sock = udp_socket_connect(LeaderIP, LeaderPort);
	socket_make_non_block(cert_sock);
	l = learner_init(paxos_config, on_deliver, NULL, base);
	assert(l != NULL);
	init_batch(&tx_batch);
	evtimer_set(&(tx_batch.timeout_ev), on_batch_timeout, &tx_batch);
	return 1;
}


int cproxy_submit(char* value, size_t size, cproxy_commit_cb cb) {
	commit_cb = cb;
	add_to_batch(&tx_batch, value, size);
	if (!BATCHING)
		send_batch(&tx_batch);
	return 1;
}


int cproxy_submit_join(int id, char* address, int port) {
	int rv;
	join_msg j;
	
	j.type = NODE_JOIN;
	j.node_id = id;
	j.port = port;
	strcpy(j.address, address);
	
	rv = send(cert_sock, &j, sizeof(join_msg), 0);
	return rv == 0;
}


int cproxy_current_st() {
	return ST;
}


void cproxy_cleanup() {
	close(cert_sock);
	print_stats();
}


static void on_init() {
	//learner_delayed_start();
}


static void handle_transaction(void* value, size_t size) {
	key k;
	val v;
    int i, offset;
    tr_id* ids;
    flat_key_val* kv;
	tr_deliver_msg* dmsg;

	LOG(VRB, ("handling transaction size %d\n",size));
	dmsg = (tr_deliver_msg*)value;

	// Report aborted / committed transactions
	ids = (tr_id*) dmsg->data;
	for (i = 0; i < dmsg->aborted_count; i++) {
		if (ids[i].node_id == NodeID) {	
			commit_cb(&ids[i], T_ABORTED);
			abort_count++;
		}
	}

	for (; i < dmsg->aborted_count + dmsg->committed_count; i++) {
		if (ids[i].node_id == NodeID) {
			commit_cb(&ids[i], T_COMMITTED);
			commit_count++;
		}
	}
	
	delivered_tx_clients += dmsg->aborted_count + dmsg->committed_count;

	// Apply updates to storage
	offset = (dmsg->aborted_count + dmsg->committed_count) * sizeof(tr_id);
	for (i = 0; i < dmsg->updateset_count; i++) {
	    kv = (flat_key_val*)&dmsg->data[offset];
	    k.size = kv->ksize;
	    k.data = kv->data;
	    v.size = kv->vsize;
	    v.data = &kv->data[k.size];
	    v.version = ST;
		
		sm_put(&k, &v);
		
	    offset += FLAT_KEY_VAL_SIZE(kv);
	}

	// Set next ST after updating storage data
	assert(dmsg->ST > ST);
	ST = dmsg->ST;
	
	delivered_tx += dmsg->aborted_count + dmsg->committed_count;
}


static void handle_join_message(join_msg* m) {
	if (m->node_id != NodeID) {
		peer_add(m->node_id, m->address, m->port);
	} else {
		printf("Joined with ST: %d\n", m->ST);
		ST = m->ST;
	}
	NumberOfNodes++;
}


struct header {
	short type;
	char data[0];
};


//typedef void (* deliver_function)(char*, size_t, iid_t, ballot_t, int, void*);
// void * is an argument to the del function
//static void on_deliver(void* value, size_t size) {
static void on_deliver(void* value, size_t size, iid_t iid,
		ballot_t ballot, int prop_id, void *arg) {
	struct header* h = (struct header*)value;
	switch (h->type) {
		case TRANSACTION_SUBMIT:
			handle_transaction(value, size);
			break;
		case NODE_JOIN:
			handle_join_message(value);
			break;
		default:
			printf("handle_request: dropping message of unkown type\n");
	}
}


static void init_batch(batch* b) {
	b->count = 0;
	b->buffer_offset = 0;
}


static void send_batch(batch* b) {
	int rv;

	rv = send(cert_sock,
			  b->buffer,
			  b->buffer_offset,
			  0);
	
	if (rv == -1)
		perror("send_batch");
	
	submitted_batch++;
	submitted_tx += b->count;

	init_batch(b);
	evtimer_del(&b->timeout_ev);
}


static void add_to_batch(batch* b, char* v, size_t s) {
	char* dst;
	int size_left;
	
	size_left = bsize - b->buffer_offset;
	
	if (size_left < s)
		send_batch(b);

	dst = &(b->buffer[b->buffer_offset]);
	memcpy(dst, v, s);
	b->count++;
	b->buffer_offset += s;
	
	if (b->count == 1) {
		evtimer_add(&b->timeout_ev, &max_batch_time);
	}
}


static void on_batch_timeout(int fd, short event, void* arg) {
	batch* b;
	b = (batch*)arg;
	send_batch(b);
	batch_timeout++;
}


static void print_stats() {
    printf("\nCERTIFIER PROXY\n");
    printf("------------------------------\n");
	if (BATCHING)
		printf("Batching: enabled\n");
	else
		printf("Batching: disabled\n");
	printf("Submitted batch: %d\n", submitted_batch);
	printf("Batches on timeout: %d\n", batch_timeout);
    printf("Submitted tx: %d\n", submitted_tx);
	printf("Delivered tx: %d\n", delivered_tx);
	printf("Delivered tx to clients: %d\n", delivered_tx_clients);
	printf("Commit count: %d\n", commit_count);
	printf("Abort count: %d\n", abort_count);
	printf("Final ST: %d\n", ST);
	printf("------------------------------\n");
//	learner_print_eventcounters();
}
