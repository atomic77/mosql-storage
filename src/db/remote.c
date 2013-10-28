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

#include "remote.h"
#include "socket_util.h"
#include "cproxy.h"
#include "storage.h"
#include "hash.h"
#include "hashtable.h"
#include "remote_msg.h"
#include "util.h"
#include "peer.h"
#include "recovery_learner_msg.h"

#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event_struct.h>
#include <event2/event_compat.h>
#include <paxos.h>

typedef struct get_request_t {
	int id;
	key* k;
	void* arg;
	int st;
	int version;
	sm_get_cb cb;
	int timeout_count;
	struct event timeout_ev;
} get_request;

static struct hashtable* requests;

static int recv_sock;
static int send_sock;
static char send_buffer[MAX_TRANSACTION_SIZE];
static char recv_buffer[MAX_TRANSACTION_SIZE];
static struct event read_ev;
// We have a rec node per acceptor, but we shouldn't assume that
// there are only three
static struct bufferevent **acc_bevs;

// Remote requests timeout
static struct timeval tv = {0, 50000};

static unsigned int request_count;
static unsigned int request_rec_count;
static unsigned int request_count_in;
static unsigned int request_completed_count;
static unsigned int request_completed_null;
static unsigned int request_drop_count;
static int request_timeout_count;

static int recovering = 0;

static int num_recs;
static int last_rec = 0;

static void on_read(int fd, short ev, void* arg);
static void handle_remote_get(remote_message* msg);
static void handle_remote_put(remote_message* msg);
static void handle_rec_key_reply(remote_message* msg);
static void send_rec_key(get_request* r);
static int send_remote_get(get_request* r, int dest_node);
static void reply_remote_get(remote_get_message* r, key* k, val* v, int cache);
static void get_request_add(get_request* r);
static int key_equal(void* k1, void* k2);
static unsigned int hash_from_key(void* k);

static void
on_rec_read(struct bufferevent* bev, void* arg) {
	int n;
	size_t blen, dlen;
	struct sockaddr_in addr;
	socklen_t addr_len;
	remote_message* rm;
	struct evbuffer *b;
	
	// Check that we have all the data
	b = bufferevent_get_input(bev);
	while((blen = evbuffer_get_length(b)) >= sizeof(size_t)) 
	{
		evbuffer_copyout(b, &dlen, sizeof(size_t));
		if (blen < dlen + sizeof(size_t)) return;
		
		evbuffer_remove(b, &dlen, sizeof(size_t));
		evbuffer_remove(b, recv_buffer, dlen); 

		rm = (remote_message*)recv_buffer;
		// FIXME We are getting the vrong message type here on recovery
		assert(rm->type == REC_KEY_REPLY); // nothing else should come this way
		handle_rec_key_reply(rm);
	}
	
}


static void on_socket_event(struct bufferevent *bev, short ev, void *arg) {
    if (ev & BEV_EVENT_CONNECTED) {
        fprintf(stdout, "remote bufferevent connected to rec\n");
    } else if (ev & BEV_EVENT_ERROR) {
        int err = EVUTIL_SOCKET_ERROR();
        fprintf(stderr, "remote bufferevent: error %d (%s)\n",
            err, evutil_socket_error_to_string(err));
    }
}

static struct bufferevent* rec_connect(struct event_base* b, 
									in_addr_t s_addr, int port) {
//									const char *address, int port) {
	struct sockaddr_in sin;
	struct bufferevent* bev;
	
	LOG(VRB,("Connecting to proposer %d : %d\n", s_addr, port));
	memset(&sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = s_addr; // inet_addr(address);
	sin.sin_port = htons(port);
	
	bev = bufferevent_socket_new(b, -1, BEV_OPT_CLOSE_ON_FREE);
	bufferevent_enable(bev, EV_READ|EV_WRITE);
	bufferevent_setcb(bev, on_rec_read, NULL, on_socket_event, NULL);
	struct sockaddr* saddr = (struct sockaddr*)&sin;
	if (bufferevent_socket_connect(bev, saddr, sizeof(sin)) < 0) {
		bufferevent_free(bev);
		return NULL;
	}
	
	return bev;
}

int remote_init(struct evpaxos_config *lp_config, struct event_base *base) {
	int i;
	struct peer* p;
	
	recv_sock = udp_bind_fd(LocalPort);
	socket_make_non_block(recv_sock);
	send_sock = udp_socket();
	socket_make_non_block(send_sock);
	
	event_set(&read_ev, recv_sock, EV_READ|EV_PERSIST, on_read, NULL);
	// event_priority_set(&read_ev, 0);
	event_add(&read_ev, NULL);
	
	// Connect to each rec (one per acceptor). For now assume rec is on
	// acceptor port + 100
	num_recs = 1; // lp_config->acceptors_count; connect to first one only
	last_rec = 0;
	
	acc_bevs = malloc(num_recs * sizeof(struct bufferevent *));
	for (i=0; i<num_recs; i++) {
//		acc_bevs[i] = rec_connect(base, lp_config->acceptors[i].address_string,
//			lp_config->acceptors[i].port+100);
		acc_bevs[i] = rec_connect(base, evpaxos_acceptor_address(lp_config, i).sin_addr.s_addr,
								  evpaxos_acceptor_listen_port(lp_config,0)+100);
	}
	
	requests = create_hashtable(512, hash_from_key, key_equal, NULL);
	request_count = 0;
	request_rec_count = 0;
	request_count_in = 0;
	request_timeout_count = 0;
	request_completed_count = 0;
	request_completed_null = 0;
	request_drop_count = 0;
	
	return 0;
}


int remote_cleanup() {
	return 0;
}


void remote_get(key* k, int ver, sm_get_cb cb, void* arg) {
	int node_id;
	int local = 0;
	get_request* req;
	
	req = malloc(sizeof(get_request));
	req->id = random();
	req->k = key_new(k->data, k->size);
	req->version = ver;
	req->st = cproxy_current_st();
	req->cb = cb;
	req->arg = arg;
	req->timeout_count = 0;
	
	node_id = peer_id_for_hash(joat_hash(k->data, k->size));
	
	if (node_id == NodeID) {
		local = 1;
		request_rec_count++;
		send_rec_key(req);
	} else {
		request_count++;
		send_remote_get(req, node_id);
	}
	
	// added this
	val* v = val_new(NULL, 0);
	storage_put(k, v, local, 1);
	val_free(v);
	
	storage_gc_stop();
	get_request_add(req);
}


void remote_start_recovery() {	
	recovering = 1;
}


static void on_read(int fd, short ev, void* arg) {
	int n;
	struct sockaddr_in addr;
	socklen_t addr_len;
	remote_message* rm;
	
	addr_len = sizeof(struct sockaddr_in);
	memset(&addr, '\0', addr_len);
	
	n = recvfrom(recv_sock,
				 recv_buffer,
				 MAX_TRANSACTION_SIZE,
				 0,
				 (struct sockaddr*)&addr,
				 &addr_len);
	
	rm = (remote_message*)recv_buffer;
	switch (rm->type) {
        case REMOTE_GET:
		handle_remote_get(rm);
        break;
		case REMOTE_PUT:
		handle_remote_put(rm);
        break;
		// We will no longer receive rec_key_reply msgs over UDP
		case REC_KEY_REPLY:
		assert(1 == 0);
		break;
		default:
		printf("Unknown message type %d\n", rm->type);
	}
}


static int send_remote_get_msg(remote_message* msg, int dest_node) {
	int size, rv;
	struct peer* p;
	struct sockaddr_in addr;
	remote_get_message* gmsg;
	
	gmsg = (remote_get_message*)msg->data;
	
	p = peer_get(dest_node);
	socket_set_address(&addr, peer_address(p), peer_port(p));
	size = REMOTE_GET_MSG_SIZE(gmsg) + sizeof(remote_message);
	
	rv = sendto(send_sock,
		   		msg,
		   		size, 
		   		0,
		   		(struct sockaddr*)&addr, 
		   		sizeof(addr));
	
	return rv;	
}


static int send_remote_get(get_request* r, int dest_node) {
	remote_message* rm;
	remote_get_message* msg;
	
	rm = (remote_message*)send_buffer;
	msg = (remote_get_message*)rm->data;
	
	rm->type = REMOTE_GET;
	
	msg->st = r->st;
	msg->req_id = r->id;
	msg->key_size = r->k->size;
	msg->version = r->version;
	msg->sender_node = NodeID;
	memcpy(msg->data, r->k->data, r->k->size);

	return send_remote_get_msg(rm, dest_node);
}


static int send_rec_key_msg(key* k, rec_key_msg* msg) {
	int size, rv;
	unsigned int h;
	struct sockaddr_in addr;
	struct peer* p;
	struct bufferevent *bev;
	
	//last_rec = (last_rec + 1) % num_recs;
	last_rec = 0; // always use the first one
	
	size = (sizeof(rec_key_msg) + msg->ksize);
	bev = acc_bevs[last_rec];
	rv = bufferevent_write(bev,msg,size);
	return rv;	
}


static void send_rec_key(get_request* r) {
	int rv;
	rec_key_msg* msg;
	
	msg = (rec_key_msg*)send_buffer;
	
	msg->type = REC_KEY_MSG;
	msg->req_id = r->id;
	msg->ksize = r->k->size;
	msg->node_id = NodeID;
	memcpy(msg->data, r->k->data, msg->ksize);
	
	rv = send_rec_key_msg(r->k, msg);
	if (rv == -1)
		printf("send_rec_key: failed to send\n");
}


static void reply_remote_get(remote_get_message* m, key* k, val* v, int cache) {
	int size, rv;
	struct peer* p;
	remote_message* rm;
	remote_put_message* msg;
	struct sockaddr_in addr;
	
	rm = (remote_message*)&send_buffer;
	msg = (remote_put_message*)rm->data;
	
	rm->type = REMOTE_PUT;
	
	msg->req_id = m->req_id;
    msg->key_size = k->size;
    msg->value_size = v->size;
    msg->val_version = v->version;
    msg->req_version = m->version;
	msg->cache = cache;
	
    memcpy(msg->data, k->data, k->size);
    memcpy(&msg->data[k->size], v->data, v->size);

	size = REMOTE_PUT_MSG_SIZE(msg) + sizeof(remote_message);
	p = peer_get(m->sender_node);
	socket_set_address(&addr, peer_address(p), peer_port(p));

	rv = sendto(send_sock,
		   		send_buffer,
	   	   		size,
		   		0,
	   	   		(struct sockaddr*)&addr,
	   	   		sizeof(addr));
	
	if (rv == -1)
		perror("sendto");
}


static void handle_deferred_remote_get(key* k, val* v, void* arg) {
	remote_get_message* m;
	m = (remote_get_message*)arg;
	reply_remote_get(m, k, v, 0);
	free(m);
}


static void handle_remote_get(remote_message* rm) {
	key k;
	val* v;
	int cache = 0;
	remote_get_message* msg;
	msg = (remote_get_message*)rm->data;
	
	request_count_in++;
		
	k.data = msg->data;
	k.size = msg->key_size;
		
	// Are we ready to handle this request?
	if (msg->version > cproxy_current_st()) {
		request_drop_count++;
		return;
	}
	
	// Reply
	v = storage_get(&k, msg->version);
	if (v == NULL) {
		request_drop_count++;
		remote_get_message* m = malloc(REMOTE_GET_MSG_SIZE(msg));
		memcpy(m, msg, REMOTE_GET_MSG_SIZE(msg));
		remote_get(&k, msg->version, handle_deferred_remote_get, m);
		return;
	}

	// The value can be cache only if there is no risk of "holes" 
	// at the receiver.
	if (cproxy_current_st() >= msg->st) {
		// check thet v is the newest item in storage
		val* newest = storage_get(&k, cproxy_current_st());
		if (newest->version == v->version)
			cache = 1;
		val_free(newest);
	}
	
	reply_remote_get(msg, &k, v, cache);
	val_free(v);
}


static void handle_remote_put(remote_message* rm) {
	key k;
	val v;
	get_request* r;
	remote_put_message* msg;
	
	msg = (remote_put_message*)rm->data;
	
	r = hashtable_remove(requests, &msg->req_id);
	if (r == NULL)
		return;

    k.size = msg->key_size;
    k.data = &msg->data[0];
    
    v.size = msg->value_size;
    v.data = &msg->data[msg->key_size];
    v.version = msg->val_version;


	if (msg->cache) {
	    if (v.size > 0) {
			int local = 0;
			if (peer_id_for_hash(joat_hash(k.data, k.size)) == NodeID)
			local = 1;
			storage_put(&k, &v, local, 1);
		} else {
			v.data = NULL;
			request_completed_null++;
		}
	}

	// Callback if necessary
	if (r->cb != NULL) {
		r->cb(&k, &v, r->arg);
	}
	
	evtimer_del(&r->timeout_ev);
	key_free(r->k);
	free(r);
	
	if (hashtable_count(requests) == 0)
		storage_gc_start();
	
	request_completed_count++;
}


static void handle_rec_key_reply(remote_message* msg) {
	int local = 0;
	get_request* r;
	rec_key_reply* rep;
	val* value = NULL;

	rep = (rec_key_reply*)msg;
	r = hashtable_remove(requests, &rep->req_id);
	if (r == NULL)
		return;
	
	if (rep->size > 0) {	
		value = versioned_val_new(rep->data, rep->size, rep->version);
		if (peer_id_for_hash(joat_hash(r->k->data, r->k->size)) == NodeID)
			local = 1;
		storage_put(r->k, value, local, 1);
	} else {
		request_completed_null++;
		value = val_new(NULL, 0);
	}	
	
	// Callback if necessary
	if (r->cb != NULL) {
		r->cb(r->k, value, r->arg);
	}
	val_free(value);
	
	evtimer_del(&r->timeout_ev);
	key_free(r->k);
	free(r);
	
	request_completed_count++;
}


static void on_request_timeout(int fd, short ev, void* arg) {
	int id;
	get_request* r;

	r = (get_request*)arg;
	r->timeout_count++;
	
	// resend the request
	id = peer_id_for_hash(joat_hash(r->k->data, r->k->size));
	if (id == NodeID) {
		send_rec_key(r);
	} else {
		send_remote_get(r, id);
	}
	
	// reschedule the timeout
	evtimer_add(&r->timeout_ev, &tv);
	request_timeout_count++;
}


static void get_request_add(get_request* r) {
	int rv;
	int* req_key;
	
	// add to hashtable
	req_key = malloc(sizeof(int));
	*req_key = r->id;
	rv = hashtable_insert(requests, req_key, r);
	assert(rv != 0);
	
	// add timeout to event loop
	evtimer_set(&r->timeout_ev, on_request_timeout, r);
	evtimer_add(&r->timeout_ev, &tv);
}


static int key_equal(void* k1, void* k2) {
	int* a = (int*)k1;
	int* b = (int*)k2;
	return (*a == *b);
}


static unsigned int hash_from_key(void* k) {
	return joat_hash(k, sizeof(int));
}


void remote_print_stats() {
	printf("Remote requests: %u\n", request_count);
	printf("Recovery requests: %u\n", request_rec_count);
	printf("Remote requests completed: %u (%u null)\n", request_completed_count, request_completed_null);
	printf("Remote requests timeout: %d\n", request_timeout_count);
	printf("Remote requests in: %u\n", request_count_in);
	printf("Remote requests dropped: %d\n", request_drop_count);
}
