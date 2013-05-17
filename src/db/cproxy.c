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
#include <errno.h>

#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <libpaxos/libpaxos.h>
#include <libpaxos/libpaxos_messages.h>
#include <evpaxos/evpaxos.h>


#define bsize MAX_TRANSACTION_SIZE
#define BATCHING 0


typedef struct batch_t {
	int count;
	char buffer[bsize];
	size_t buffer_offset;
	struct event *timeout_ev;
} batch;

static batch tx_batch;
static struct timeval max_batch_time = {0, 1000};
static cproxy_commit_cb commit_cb;

static int ST;
static struct bufferevent *cert_bev;
static struct event_base *base;
static int submitted_batch = 0;
static int batch_timeout = 0;
static int submitted_tx = 0;
static int delivered_tx = 0;
static int delivered_tx_clients = 0;
static int abort_count = 0;
static int commit_count = 0;


static void on_init();
static void on_deliver(char* value, size_t size, iid_t iid,
		ballot_t ballot, int prop_id, void *arg) ;
static void on_batch_timeout(int fd, short event, void* arg);
static void init_batch(batch* b);
static void send_batch(batch* b);
static void add_to_batch(batch* b, char* v, size_t s);
static void print_stats();

static void on_socket_event(struct bufferevent *bev, short ev, void *arg) {
    if (ev & BEV_EVENT_CONNECTED) {
        fprintf(stdout, "cproxy bufferevent connected\n");
    } else if (ev & BEV_EVENT_ERROR) {
        int err = EVUTIL_SOCKET_ERROR();
        fprintf(stderr, "cproxy: error %d (%s)\n",
            err, evutil_socket_error_to_string(err));
    }
}

static struct bufferevent* cm_connect(struct event_base* b, 
									const char *address, int port) {
	struct sockaddr_in sin;
	struct bufferevent* bev;
	
	LOG(VRB,("Connecting to proposer %s : %d\n", address, port));
	memset(&sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = inet_addr(address);
	sin.sin_port = htons(port);
	
	bev = bufferevent_socket_new(b, -1, BEV_OPT_CLOSE_ON_FREE);
	bufferevent_enable(bev, EV_WRITE);
	bufferevent_setcb(bev, NULL, NULL, on_socket_event, NULL);
	struct sockaddr* saddr = (struct sockaddr*)&sin;
	if (bufferevent_socket_connect(bev, saddr, sizeof(sin)) < 0) {
		bufferevent_free(bev);
		return NULL;
	}
	return bev;
}

int cproxy_init(const char* paxos_config, struct event_base *b) {
	struct learner *l;
	ST = 0;
	base = b; 
	cert_bev = cm_connect(base, LeaderIP, LeaderPort);
	assert(cert_bev != NULL);
	l = evlearner_init(paxos_config, on_deliver, NULL, base);
	assert(l != NULL);
	init_batch(&tx_batch);
 	tx_batch.timeout_ev = evtimer_new(base, on_batch_timeout, &tx_batch);
	assert(tx_batch.timeout_ev != NULL);
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
	strncpy(j.address, address, 17);
	
	bufferevent_write(cert_bev, &j, sizeof(join_msg));
	return rv == 0;
}


int cproxy_current_st() {
	return ST;
}


void cproxy_cleanup() {
	bufferevent_free(cert_bev);
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


static void on_deliver(char* value, size_t size, iid_t iid,
		ballot_t ballot, int prop_id, void *arg) {
	struct header* h = (struct header*)value;
	switch (h->type) {
		case TRANSACTION_SUBMIT:
			handle_transaction(value, size);
			break;
		case NODE_JOIN:
			handle_join_message((join_msg *)value);
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

	rv = bufferevent_write(cert_bev, b->buffer, b->buffer_offset);
	
	submitted_batch++;
	submitted_tx += b->count;

	init_batch(b);
	evtimer_del(b->timeout_ev);
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
		assert(b->timeout_ev != NULL);
		evtimer_add(b->timeout_ev, &max_batch_time);
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
