#include "dsmDB_priv.h"
#include "storage.h"
#include <libpaxos.h>
#include <libpaxos/libpaxos_messages.h>
#include <libpaxos/config_reader.h>
#include "config_reader.h"
#include "socket_util.h"
#include "index.h"
#include "recovery_learner_msg.h"
#include "peer.h"
#include "hash.h"
#include "tapiocadb.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <signal.h>
#include <event2/event_compat.h>
#include <event2/event.h>
#include <event2/event_struct.h>


struct header {
	short type;
	char data[0];
};


#define VERBOSE 0
#define buffer_size MAX_TRANSACTION_SIZE

static void handle_rec_key();

typedef void (*handler)(char*, int);
static handler handle[] = 
	{ handle_rec_key };

static rlog *rl;
static struct storage *ssm = NULL;
static int aid;
static int recv_sock;
static int send_sock;
static char recv_buffer[buffer_size];
static char send_buffer[buffer_size];
static struct event_base *base;

static iid_t Iid = 0;
static int rec_key_count = 0;
static int rec_fsync_interval = 5000;

static void on_deliver(void* value, size_t size, iid_t iid,
		ballot_t ballot, int prop_id, void *arg) ;
void update_rec_index(iid_t iid, tr_deliver_msg* dmsg);

static void print_rec_key_msg(rec_key_msg* rm, int size) {
	if (!VERBOSE) return;
	printf("** rec_key_msg (%d bytes):\n", size);
	printf("   node id: %d\n", rm->node_id);
	printf("   key size: %d\n", rm->ksize);
	if (rm->ksize == sizeof(int)) {
		printf("   key: %d\n", *(int*)rm->data);
	} else {
		printf("   key: %d bytes\n", rm->ksize);
	}
}


static void index_entry_print(key* k, off_t off) {
	if (!VERBOSE) return;
	if (off == -1) {
		printf("** no index entry for key %d\n", *(int*)k->data);
		return;
	}
	printf("** index entry for key %d\n", *(int*)k->data);
	printf("   IID: %lld\n", off);
}


void send_rec_key_reply(int id, rec_key_reply* rep) {
	int size;
	struct peer* p;
	struct sockaddr_in addr;
	
	p = peer_get(id);
	if (p == NULL) {
		printf("Peer %d not found. dropping request\n", id);
		return;
	}
	socket_set_address(&addr, peer_address(p), peer_port(p));
	size = rep->size + sizeof(rec_key_reply);
	
	sendto(send_sock,
			rep,
			size, 
			0,
			(struct sockaddr*)&addr, 
			sizeof(addr));
}


/*
static void prepare_reply_data(key* k, paxos_msg* rec, rec_key_reply* reply) {
	int i, offset;
	flat_key_val* kv;
	tr_deliver_msg* dmsg;
	
	dmsg = (tr_deliver_msg*)rec->cmd_value;
	offset = (dmsg->aborted_count + dmsg->committed_count) * sizeof(tr_id);
	
	for (i = 0; i < dmsg->updateset_count; i++) {
    	kv = (flat_key_val*)&dmsg->data[offset];
		if ((kv->ksize == k->size) && (memcmp(kv->data, k->data, k->size) == 0)) {
			reply->size = kv->vsize;
			reply->version = dmsg->ST;
			memcpy(reply->data, &kv->data[kv->ksize], kv->vsize);
		}
    	offset += FLAT_KEY_VAL_SIZE(kv);
	}
}
*/


static void prepare_reply_data(key* k, tr_deliver_msg* dmsg, rec_key_reply* reply) {
	int i, offset;
	flat_key_val* kv;
//	tr_deliver_msg* dmsg;
//	dmsg = (tr_deliver_msg*)rec->cmd_value;

	offset = (dmsg->aborted_count + dmsg->committed_count) * sizeof(tr_id);

	for (i = 0; i < dmsg->updateset_count; i++) {
    	kv = (flat_key_val*)&dmsg->data[offset];
		if ((kv->ksize == k->size) && (memcmp(kv->data, k->data, k->size) == 0)) {
			reply->size = kv->vsize;
			reply->version = dmsg->ST;
			memcpy(reply->data, &kv->data[kv->ksize], kv->vsize);
		}
    	offset += FLAT_KEY_VAL_SIZE(kv);
	}
}


static void handle_rec_key(char* buffer, int size) {
	key k;
	iid_t iid;
//	paxos_msg* rec;
	tr_deliver_msg *dmsg;
	rec_key_msg* rm;
	rec_key_reply* rep;
	
	rec_key_count++;
	
	rm = (rec_key_msg*)buffer;
	rep = (rec_key_reply*)send_buffer;
	
	if (size <= (sizeof(rec_key_msg))) {
		printf("Error: Ignoring rec_key_msg of wrong size, got %d, expect %d\n",
				size, sizeof(rec_key_msg));
		assert(false);
		return;
	}
	
	if (size != (sizeof(rec_key_msg) + rm->ksize)) {
		printf("Error: Ignoring rec_key_msg of wrong size, got %d, expect %d + %d\n",
				size, (sizeof(rec_key_msg)) , rm->ksize);
		assert(false);
		return;
	}
	
	print_rec_key_msg(rm, size);
	
	// Lookup the index
	k.size = rm->ksize;
	k.data = rm->data;
	iid = rlog_read(rl, &k);
	if (iid != -1) {
		index_entry_print(&k, iid);
		accept_ack *ar = storage_get_record(ssm, iid);
		if (ar == NULL) {
			fprintf(stderr, "Paxos log read error on iid %d \n", iid);
			assert(1337 == 0xDEADBEEF);
			return;
		}
		assert(ar->value_size > 0);
		dmsg = (tr_deliver_msg *)ar->value;
/*		rec = (accept_ack *) malloc(ACCEPT_ACK_SIZE(ar) + sizeof(paxos_msg));
		rec-> = ir->inst_number;
		rec->ballot = ir->accept_ballot;
		rec->cmd_key = ir->accepted_cmd_key;
		rec->cmd_size = ir->accepted_cmd_size;
		memcpy(rec->value, ar->value, ir->value_size);*/

		rep->type = REC_KEY_REPLY;
		rep->req_id = rm->req_id;
		prepare_reply_data(&k,dmsg,rep);
		free(ar);
	} else {
		rep->type = REC_KEY_REPLY;
		rep->req_id = rm->req_id;
		rep->size = 0;
	}
	
	send_rec_key_reply(rm->node_id, rep);
}


static int key_belongs_here(key* k) {
	unsigned int h;
	h = joat_hash(k->data, k->size);
	if (aid == 2)
		return ((h % 2) == 0);
	else
		return ((h % 2) == 1);
}

void update_rec_index(iid_t iid, tr_deliver_msg* dmsg)
{
	key k;
	int i, byte;
	flat_key_val* kv;
	// We are not interested in transactions ids
	byte = (sizeof(tr_id) * (dmsg->aborted_count + dmsg->committed_count));
	// Apply updates to index
	for (i = 0; i < dmsg->updateset_count; i++)
	{
		kv = (flat_key_val*) &dmsg->data[byte];
		k.size = kv->ksize;
		k.data = kv->data;
//		if (key_belongs_here(&k)) {
			rlog_update(rl, &k, iid);
//		}
		byte += FLAT_KEY_VAL_SIZE(kv);
		if ( (rec_key_count + 1) % rec_fsync_interval == 0){
			rlog_sync(rl);
		}
	}
}

static void handle_transaction(void* value, size_t size, iid_t iid) {
	tr_deliver_msg* dmsg;
	dmsg = (tr_deliver_msg*)value;
	update_rec_index(iid, dmsg);
}


static void handle_join_message(join_msg* m) {
	peer_add(m->node_id, m->address, m->port);
	NumberOfNodes++;
}


static void on_deliver(void* value, size_t size, iid_t iid,
		ballot_t ballot, int prop_id, void *arg) {
	Iid++; // Update global instance id
	
	printf("Rec learned value size %d\n", size); 
	struct header* h = (struct header*)value;
	switch (h->type) {
		case TRANSACTION_SUBMIT:
			handle_transaction(value, size, iid);
			break;
		case NODE_JOIN:
			handle_join_message(value);
			break;
	}
}


static void on_request(int fd, short ev, void* arg) {
	int n;
	int* type;
	socklen_t addr_len;
	struct sockaddr_in addr;
	
	addr_len = sizeof(struct sockaddr_in);
	memset(&addr, '\0', addr_len);
	
	n = recvfrom(recv_sock,
				 recv_buffer,
				 buffer_size,
				 0,
				 (struct sockaddr*)&addr,
				 &addr_len);
	assert(n != -1);
	
	// We expect the "type" to be in the first 4 bytes
	type = ((int*)recv_buffer);
	if (*type >= (sizeof(handle) / sizeof(handler)) || *type < 0) {
		printf("Error: ignoring message of type %d\n", *type);
		return;
	}
	
	handle[*type](recv_buffer, n);
}

// TODO Deprecated now that BDB working correctly; may need some variation of
// this logic for recovery however
void reload_keys() {
	int rv, byte, n;
	iid_t iid;
	tr_deliver_msg *dmsg;
	n = 0;
	for(;;)
	{
//		rv = plog_next(pl, &m, &iid);
//		dmsg = (tr_deliver_msg *)m->cmd_value;
		update_rec_index(iid, dmsg);
		n++;
		if(iid == -1) break;
	}
	printf("PLOG: Loaded %d keys from BDB\n", n);
}

void sigint(int sig) {
	printf("IID %lu\n", Iid);
	printf("Rec key count %d\n", rec_key_count);
//	printf("Index count %d\n", rlog_num_keys());
	rlog_close(rl);
	storage_close(ssm);
	exit(0);
}


static void init(int acceptor_id, const char* paxos_conf, const char* tapioca_conf, int port) {
	char log_path[128], rec_db_path[128];

	struct event request_ev;

	tapioca_init_defaults();
	signal(SIGINT, sigint);
	load_config_file(tapioca_conf);
	
	
	// FIXME The rec is not learning properly likely due to the mixture of
	// compatibility mode libevent and the event2 stuff used by libpaxos; 
	// need to clean this up
	event_init();
	
	aid = acceptor_id;
	
	recv_sock = udp_bind_fd(port);
	socket_make_non_block(recv_sock);
	event_set(&request_ev, recv_sock, EV_READ|EV_PERSIST, on_request, NULL);
	event_add(&request_ev, NULL);
	
	send_sock = udp_socket();
	socket_make_non_block(send_sock);
	base = event_base_new();

	struct learner *l = learner_init(paxos_conf, on_deliver, NULL, base);
	assert(l != NULL);
	
	// Open acceptor logs
    ssm = storage_open(acceptor_id, 0);
    assert(ssm != NULL);

	sprintf(rec_db_path, "%s/rlog_%d", "/tmp", acceptor_id);
	rl = rlog_init(rec_db_path);
	assert(rl != NULL);

/*	// Reload any keys that happen to be in the BDB log
	reload_keys();*/
	event_dispatch();

}


int main(int argc, char const *argv[]) {
	int port = 12345;
	if (argc < 4 || argc > 5) {
		printf("Usage: %s <acceptor id> <paxos config> <tapioca config> <port>\n", argv[0]);
		return 1;
	} else {
		if (argc == 5)
			port = atoi(argv[4]);
		init(atoi(argv[1]), argv[2], argv[3], port);
		return 0;
	}
}
