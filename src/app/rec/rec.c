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

#include "dsmDB_priv.h"
#include <libpaxos/storage.h>
#include <libpaxos.h>
#include <libpaxos/libpaxos_messages.h>
#include <evpaxos/config_reader.h>
#include "config_reader.h"
#include "socket_util.h"
#include "index.h"
#include "recovery_learner_msg.h"
#include "peer.h"
#include "hash.h"
#include "tapiocadb.h"
#include "carray.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <signal.h>
#include <errno.h>
#include <event2/listener.h>
#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>

struct header {
	short type;
	char data[0];
};


#define VERBOSE 0

static rec_key_reply * handle_rec_key(rec_key_msg *rm) ;

// The rec only responds to one type of message so this is unnecessary atm
//typedef void (*handler)(char*, int);
//static handler handle[] = 
//	{ handle_rec_key };

static rlog *rl;
static struct storage *ssm = NULL;
static int aid;
static int recv_sock;
static int send_sock;
static char recv_buffer[MAX_TRANSACTION_SIZE];
static char send_buffer[MAX_TRANSACTION_SIZE];
static struct event_base *base;
static struct carray* bevs;

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

static void prepare_reply_data(key* k, tr_deliver_msg* dmsg, rec_key_reply* reply) {
	int i, offset;
	flat_key_val* kv;

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

static rec_key_reply * handle_rec_key(rec_key_msg *rm) {
	key k;
	iid_t iid;
	tr_deliver_msg *dmsg;
	rec_key_reply* rep;
	
	rec_key_count++;
	
	rep = (rec_key_reply*)send_buffer;
	
	// Lookup the index
	k.size = rm->ksize;
	k.data = rm->data;
	rlog_tx_begin(rl);
	iid = rlog_read(rl, &k);
	rlog_tx_commit(rl);
	if (iid > 0) {
		index_entry_print(&k, iid);
		storage_tx_begin(ssm);
		accept_ack *ar = storage_get_record(ssm, iid);
		storage_tx_commit(ssm);
		if (ar == NULL) {
			LOG(VRB, ("Paxos log read error on iid %d \n", iid));
			assert(1337 == 0xDEADBEEF);
			return;
		}
		if(ar->value_size == 0) {
			printf("acc. record is final %d iid %d ballot %d\n" , ar->is_final, ar->iid, ar->ballot);
			assert(11337 == 0xDBCAFE);
		}
		dmsg = (tr_deliver_msg *)ar->value;

		rep->type = REC_KEY_REPLY;
		rep->req_id = rm->req_id;
		prepare_reply_data(&k,dmsg,rep);
		//free(ar);
	} else {
		rep->type = REC_KEY_REPLY;
		rep->req_id = rm->req_id;
		rep->size = 0;
	}
	
	return rep;
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
			rlog_tx_begin(rl);
			rlog_update(rl, &k, iid);
			rlog_tx_commit(rl);
//		}
		byte += FLAT_KEY_VAL_SIZE(kv);
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
	
	LOG(VRB, ("Rec learned value size %d\n", size)); 
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


// TODO Deprecated now that BDB working correctly; may need some variation of
// this logic for recovery however
void reload_keys() {
	int rv, byte, n;
	iid_t iid;
	tr_deliver_msg *dmsg;
	n = 0;
	for(;;)
	{
		update_rec_index(iid, dmsg);
		n++;
		if(iid == -1) break;
	}
	LOG(VRB, ("PLOG: Loaded %d keys from BDB\n", n));
}

void sigint(int sig) {
	printf("IID %lu\n", Iid);
	printf("Rec key count %d\n", rec_key_count);
//	printf("Index count %d\n", rlog_num_keys());
	rlog_close(rl);
	storage_close(ssm);
	exit(0);
}

static void on_rec_request(struct bufferevent* bev, void* arg)
{
	size_t len, rlen;
	struct evbuffer* b;
	rec_key_msg rm;
	rec_key_reply *rep;
	
	b = bufferevent_get_input(bev);
	while((len = evbuffer_get_length(b)) >= sizeof(rec_key_msg)) 
	{
		evbuffer_copyout(b, &rm, sizeof(rec_key_msg));
		if (len < sizeof(rec_key_msg) + rm.ksize) return;
		
		evbuffer_remove(b, recv_buffer, sizeof(rec_key_msg) + rm.ksize); 

		rep = handle_rec_key((rec_key_msg *)recv_buffer);
		rlen = rep->size + sizeof(rec_key_reply);
		bufferevent_write(bev,&rlen , sizeof(size_t));
		bufferevent_write(bev, rep, rep->size + sizeof(rec_key_reply));
	}
}


static void
on_bev_error(struct bufferevent *bev, short events, void *arg)
{
	if (events & BEV_EVENT_ERROR)
		perror("Error from bufferevent");
	if (events & (BEV_EVENT_EOF|BEV_EVENT_ERROR))
		bufferevent_free(bev);
}

static void
on_connect(struct evconnlistener *l, evutil_socket_t fd,
	struct sockaddr *addr, int socklen, void *arg)
{
	struct event_base* b = evconnlistener_get_base(l);
	struct bufferevent *bev = bufferevent_socket_new(b, fd, 
		BEV_OPT_CLOSE_ON_FREE);
	bufferevent_setcb(bev, on_rec_request, NULL, on_bev_error, arg);
	bufferevent_enable(bev, EV_READ);
 	carray_push_back(bevs, bev);
	LOG(VRB, ("accepted connection from...\n"));
}

static void
on_listener_error(struct evconnlistener* l, void* arg)
{
	struct event_base *base = evconnlistener_get_base(l);
	int err = EVUTIL_SOCKET_ERROR();
	fprintf(stderr, "Got an error %d (%s) on the listener. "
		"Shutting down.\n", err, evutil_socket_error_to_string(err));

	event_base_loopexit(base, NULL);
}

struct evconnlistener *
bind_new_listener(struct event_base* b, address* a,
 	evconnlistener_cb conn_cb, evconnlistener_errorcb err_cb)
{
	struct evconnlistener *el;
	struct sockaddr_in sin;
	unsigned flags = LEV_OPT_CLOSE_ON_EXEC
		| LEV_OPT_CLOSE_ON_FREE
		| LEV_OPT_REUSEABLE;
	
	memset(&sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = inet_addr(a->address_string);
	sin.sin_port = htons(a->port);
	el = evconnlistener_new_bind(
		b, conn_cb, NULL, flags, -1, (struct sockaddr*)&sin, sizeof(sin));
	assert(el != NULL);
	evconnlistener_set_error_cb(el, err_cb);
 	bevs = carray_new(10);
	
	return el;
}


static void init(int acceptor_id, const char* paxos_conf, const char* tapioca_conf, int port) {
	char log_path[128], rec_db_path[128];

// 	struct event request_ev;

	tapioca_init_defaults();
	signal(SIGINT, sigint);
	load_config_file(tapioca_conf);
	
	aid = acceptor_id;
	base = event_base_new();
	
	// Start learner
	struct learner *l = evlearner_init(paxos_conf, on_deliver, NULL, base);
	assert(l != NULL);
	
	// Create new listener for recovery requests
	struct config* conf = read_config(paxos_conf);
	address a;
	a.address_string = "0.0.0.0";
	// For now define a rec as listening on acceptor port + 100
	a.port = conf->acceptors[acceptor_id].port+100;
	struct evconnlistener *el =  bind_new_listener(base, &a, on_connect, on_listener_error);

	// Open acceptor logs
    ssm = storage_open(acceptor_id, 1);
    assert(ssm != NULL);

	sprintf(rec_db_path, "%s/rlog_%d", "/tmp", acceptor_id);
	rl = rlog_init(rec_db_path);
	assert(rl != NULL);
	
	event_base_dispatch(base);

/*	// Reload any keys that happen to be in the BDB log
	reload_keys();*/
// 	event_dispatch();

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
