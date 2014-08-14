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
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/queue.h>
#include <string.h>
#include <assert.h>
#include <event2/event.h>
#include <event2/event_struct.h>
#include <event2/buffer.h>
#include <event2/event_compat.h>
#include <event2/bufferevent.h>
#include <event2/bufferevent_compat.h>
#include <paxos.h>

#include "tcp.h"
#include "http.h"
#include "hash.h"
#include "hashtable.h"
#include "bplustree.h"


typedef struct tcp_client_t {
	int id;
	int fd;
	transaction* t;
	struct sockaddr_in addr;
	struct event timeout_ev;
	struct bufferevent* buffer_ev;
	int write_count;
	bptree_key_val kv_prev;
} tcp_client;


typedef struct listener_t {
	int fd;
	struct event accept_ev;
	struct sockaddr_in addr;
} listener;

static int is_socket_init = 0;

static listener ls;
static struct timeval commit_timeout = {300, 500000};


static int init_listener(listener* l, int port);
static void on_accept(int fd, short ev, void *arg);
static tcp_client* tcp_client_new();
static void tcp_client_free(tcp_client* c);
static int tcp_client_remove(int cid);
static int setnonblock(int fd);
static int key_equal(void* k1, void* k2);
static unsigned int hash_from_key(void* k);

//static void on_commit(tr_id* id, int commit_result);
static void send_result(struct bufferevent* bev, int res);

static void on_read(struct bufferevent *bev, void *arg);
static void on_write(struct bufferevent *bev, void *arg);
static void on_error(struct bufferevent *bev, short what, void *arg);

static void handle_open(tcp_client* c, struct evbuffer* b);
static void handle_close(tcp_client* c, struct evbuffer* b);
static void handle_commit(tcp_client* c, struct evbuffer* b);
static void handle_rollback(tcp_client* c, struct evbuffer* b);
static void handle_get(tcp_client* c, struct evbuffer* b);
static void handle_put(tcp_client* c, struct evbuffer* b);
static void handle_mget(tcp_client* c, struct evbuffer* buffer);
static void handle_mput(tcp_client* c, struct evbuffer* buffer);
static void handle_mget_put(tcp_client* c, struct evbuffer* buffer);


/********************************************************/
/* New B+Tree stuff */

typedef struct client_bpt_id {
	int id;
	uint16_t bpt_id;
} client_bpt_id;

struct hashtable *client_bpt_ids;

static int client_bpt_id_cmp(void* k1, void* k2) {
	client_bpt_id* a = (client_bpt_id *)k1;
	client_bpt_id* b = (client_bpt_id *)k2;
	return (a->id == b->id &&
			a->bpt_id == b->bpt_id);
}
static void send_bptree_result(tcp_client *c, int rv, int32_t vsize, void *v);


static void handle_bptree_initialize_bpt_session_no_commit(tcp_client* c,
		struct evbuffer* buffer);
static void handle_bptree_initialize_bpt_session(tcp_client* c,
		struct evbuffer* buffer);
static void handle_bptree_set_num_fields(tcp_client* c,struct evbuffer* buffer);
static void handle_bptree_set_field_info(tcp_client* c,struct evbuffer* buffer);
static void handle_bptree_insert(tcp_client* c,struct evbuffer* buffer);
static void handle_bptree_update(tcp_client* c,struct evbuffer* buffer);
static void handle_bptree_search(tcp_client* c,struct evbuffer* buffer);
static void handle_bptree_index_first(tcp_client* c,struct evbuffer* buffer);
static void handle_bptree_index_next(tcp_client* c,struct evbuffer* buffer);
static void handle_bptree_index_next_mget(tcp_client* c,struct evbuffer* buffer);
static void handle_bptree_index_first_no_key(tcp_client* c,
		struct evbuffer* buffer);
static void handle_bptree_debug(tcp_client* c, struct evbuffer* buffer);
static void handle_bptree_delete(tcp_client* c,struct evbuffer* buffer);

static unsigned int hash_from_client_bpt_key(void* k);

/* End new B+Tree stuff */
/********************************************************/
#define BPTREE_MESSAGE_TYPE_START 14
#define BPTREE_MESSAGE_TYPE_END 26

typedef void (*handler)(tcp_client* c, struct evbuffer* b);
static handler handle[] = 
	{ handle_open,
	  handle_close,
	  handle_commit,
	  handle_rollback,
	  handle_get,
	  handle_put,
	  handle_mget,
	  handle_mput,
	  handle_mget_put,
	  // Previously used for simple btree functions
	  NULL,
	  NULL,
	  NULL,
	  NULL,
	  // New B+Tree operations
	  NULL, // previously used for test_op
	  handle_bptree_initialize_bpt_session_no_commit, /* 14 */
	  handle_bptree_initialize_bpt_session,
	  handle_bptree_set_num_fields,
	  handle_bptree_set_field_info,
	  handle_bptree_insert,
	  handle_bptree_update,
	  handle_bptree_search,
	  handle_bptree_index_first,
	  handle_bptree_index_next,
	  handle_bptree_index_next_mget,
	  handle_bptree_index_first_no_key,
	  handle_bptree_debug,
	  handle_bptree_delete /* 26 */

	};


int tcp_init(int port) {
	int rv = 0;
	rv = init_listener(&ls, port);
	if (rv < 0) return -1;
	clients = create_hashtable(256, hash_from_key, key_equal, free);
	if (clients == NULL) return -1;
	// Alex: Add initialization of client-bpt map here
	client_bpt_ids =
			create_hashtable(256, hash_from_client_bpt_key, client_bpt_id_cmp,free);
	if (client_bpt_ids == NULL) return -1;
	return 0;
}


static int init_listener(listener* l, int port) {
	int yes = 1;
	l->fd = socket(AF_INET, SOCK_STREAM, 0);
	if (l->fd < 0) return -1;
	setsockopt(l->fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));
	if (setnonblock(l->fd) < 0)
		return -1;
	memset(&l->addr, 0, sizeof(l->addr));
	l->addr.sin_family = AF_INET;
	l->addr.sin_addr.s_addr = INADDR_ANY;
	l->addr.sin_port = htons(port);
	if (bind(l->fd, (struct sockaddr *)&l->addr, sizeof(l->addr)) < 0)
		return -1;
	if (listen(l->fd, 128) < 0)
		return -1;
    event_set(&l->accept_ev, l->fd, EV_READ|EV_PERSIST, on_accept, NULL);
    event_add(&l->accept_ev, NULL);
	return 0;
}


static void on_accept(int fd, short ev, void *arg) {
	int rv, *k;
	tcp_client* c;
	unsigned int addr_len;
	is_socket_init = 1;
	
	c = tcp_client_new();
	k = malloc(sizeof(int));
	*k = c->id;
	rv = hashtable_insert(clients, k, c);
	
	addr_len = sizeof(c->addr);
	c->fd = accept(fd, (struct sockaddr*)&c->addr, &addr_len);
	if (c->fd < 0) {
		// TODO free client
		return;
	}
	
	setnonblock(c->fd); // TODO check return value
	
	c->buffer_ev = bufferevent_new(c->fd, on_read, on_write, on_error, c);
	bufferevent_enable(c->buffer_ev, EV_READ);
	c->write_count = 0;
}


static int message_incomplete(struct evbuffer* b) {
	int bsize;
	int msize;
	bsize = evbuffer_get_length(b);
	if (bsize < sizeof(int))
		return 1;
	evbuffer_copyout(b, &msize, sizeof(int));
	if (bsize < (2*sizeof(int) + msize))
		return 1;
	return 0;
}

static int bptree_message_incomplete(struct evbuffer *b) {
	// BTree messages also should contain a bpt_id
	int bsize = evbuffer_get_length(b);
	if (bsize < sizeof(uint16_t)) return 1;
	return 0;
}


static void on_read(struct bufferevent *bev, void *arg)  {
	int size, type;
	tcp_client* c = (tcp_client*)arg;
	struct evbuffer* input;
	assert(is_socket_init);
	
	input = bufferevent_get_input(bev);
	
	if (message_incomplete(input))
		return;
	
	evbuffer_remove(input, &size, sizeof(int));
	evbuffer_remove(input, &type, sizeof(int));
	if (type >= BPTREE_MESSAGE_TYPE_START && type <= BPTREE_MESSAGE_TYPE_END){
		if (bptree_message_incomplete(input)) return;
	}

	if (type >= (sizeof(handle) / sizeof(handler)) || type < 0) {
		printf("Error: ignoring message of type %d\n", type);
		return;
	}
//	printf("GOT MESSASGE TYPE %d\n",type);
//	fflush(stdout);
	handle[type](c, input);
}


static void on_write(struct bufferevent *bev, void *arg) { }


static void on_error(struct bufferevent *bev, short what, void *arg) {
	tcp_client*c = (tcp_client*)arg;
	tcp_client_remove(c->id);
}


static void handle_open(tcp_client* c, struct evbuffer* b) {
	int rv = 1, size = 2*sizeof(int);
	struct evbuffer* rep = evbuffer_new();
	
	evbuffer_add(rep, &size, sizeof(int));
	evbuffer_add(rep, &rv, sizeof(int));
	
	evbuffer_add(rep, &c->id, sizeof(int));
	evbuffer_add(rep, &NodeID, sizeof(int));
	bufferevent_write_buffer(c->buffer_ev, rep);
	evbuffer_free(rep);
}


static void handle_close(tcp_client* c, struct evbuffer* b) { 
	int rv = 1, size = 0;
	struct evbuffer* rep = evbuffer_new();
	evbuffer_add(rep, &size, sizeof(int));
	evbuffer_add(rep, &rv, sizeof(int));
	bufferevent_write_buffer(c->buffer_ev, rep);
	evbuffer_free(rep);
}


static void handle_commit(tcp_client* c, struct evbuffer* b) { 
	int rv = transaction_remote_count(c->t);
	
	if (transaction_read_only(c->t)) {
		send_result(c->buffer_ev, rv);
		transaction_clear(c->t);
		return;
	}
	
 	rv = transaction_commit(c->t, c->id, on_commit);
	if (rv < 0) {
		transaction_clear(c->t);
		send_result(c->buffer_ev, -1);
	}
	evtimer_add(&c->timeout_ev, &commit_timeout);
}


static void handle_rollback(tcp_client* c, struct evbuffer* b) {
	transaction_clear(c->t);
	send_result(c->buffer_ev, 1);
}


//static
void on_commit(tr_id* id, int commit_result) {
	tcp_client* c;
	int rv = commit_result;

	c = hashtable_search(clients, &id->client_id);
	if (c == NULL) return;
	c->write_count = 0;

	// TODO  checking the sequence number here is not sufficient. 
	// Suppose a timeout occurs, the sequence number remains unchanged until
	// the next call to transaction commit...
	if (id->seqnumber != c->t->id.seqnumber)
		return;
	
	if (commit_result == T_COMMITTED)
		rv = transaction_remote_count(c->t);

#ifdef TRACE_MODE
	write_to_trace_file(commit_result,id,NULL,NULL,0);
#endif
	
	send_result(c->buffer_ev, rv);
	evtimer_del(&c->timeout_ev);
	transaction_clear(c->t);
}


static void on_commit_timeout(int fd, short event, void* arg) {
	tcp_client* c = (tcp_client*)arg;
	transaction_clear(c->t);
	send_result(c->buffer_ev, -1);
	printf("Commit timeout passed! Probably should abort for safety\n");
	//assert(0);
}


static void execute_put(transaction* t, void* k, int ksize, void* v, int vsize) {
	key _k;
	val _v;
	_k.size = ksize;
	_k.data = k;
	_v.size = vsize;
	_v.data = v;
	transaction_put(t, &_k, &_v);
}


static void handle_put(tcp_client* c, struct evbuffer* b) { 
	int ksize, vsize;
	char k[MAX_TRANSACTION_SIZE];
	char v[MAX_TRANSACTION_SIZE];
	
	// TODO check return values
	evbuffer_remove(b, &ksize, sizeof(int));
	evbuffer_remove(b, k, ksize);
	evbuffer_remove(b, &vsize, sizeof(int));
	evbuffer_remove(b, v, vsize);
		
	execute_put(c->t, k, ksize, v, vsize);
	send_result(c->buffer_ev, 1);
}


static void send_get_result(tcp_client* c, val* v) {
	int rv, size;
	struct evbuffer* b;
	if (v == NULL) {
		send_result(c->buffer_ev, -1);
		return;
	}
	
	rv = 1;
	size = v->size + sizeof(int);
	b = evbuffer_new();
	evbuffer_add(b, &size, sizeof(int));
	evbuffer_add(b, &rv, sizeof(int));
	evbuffer_add(b, &v->size, sizeof(int));
	evbuffer_add(b, v->data, v->size);
	bufferevent_write_buffer(c->buffer_ev, b);
	evbuffer_free(b);
}


static void on_get(key* k, val* v, void* arg) {
	tcp_client* c = (tcp_client*)arg;
	handle_get(c, bufferevent_get_input(c->buffer_ev));
}


static val* execute_get(transaction* t, void* k, int ksize) {
	key _k;
	_k.data = k;
	_k.size = ksize;
	return transaction_get(t, &_k);
}


static struct evbuffer* evbuffer_copy(struct evbuffer* b) {
	size_t size;
	unsigned char* data;
	struct evbuffer* copy;
	copy = evbuffer_new();
	size = evbuffer_get_length(b);
	data = evbuffer_pullup(b, size);
	evbuffer_add(copy, data, size);
	return copy;
}


static void handle_get(tcp_client* c, struct evbuffer* buffer) {
	int ksize;
	char k[MAX_TRANSACTION_SIZE];
	val* v;
	struct evbuffer* b = evbuffer_copy(buffer);
	evbuffer_remove(b, &ksize, sizeof(int));
	evbuffer_remove(b, k, ksize);
	transaction_set_get_cb(c->t, on_get, c);
	v = execute_get(c->t, k, ksize);
	if (v != NULL) {
		evbuffer_drain(buffer, evbuffer_get_length(buffer));
		send_get_result(c, v);
		val_free(v);
	}
	evbuffer_free(b);
}


static void on_mget(key* k, val* v, void* arg) {
	tcp_client* c = (tcp_client*)arg;
	handle_mget(c, bufferevent_get_input(c->buffer_ev));
}


static void send_mget_result(tcp_client* c, int rv, int n, val** values) {
	int size = 0, i;
	struct evbuffer* b = evbuffer_new();
	for (i = 0; i < n; i++)
		size += values[i]->size + sizeof(int);
	size += sizeof(int);
	evbuffer_add(b, &size, sizeof(int));
	evbuffer_add(b, &rv, sizeof(int));
	evbuffer_add(b, &n, sizeof(int));
	for (i = 0; i < n; i++) {
		evbuffer_add(b, &values[i]->size, sizeof(int));
		evbuffer_add(b, values[i]->data, values[i]->size);
	}
	bufferevent_write_buffer(c->buffer_ev, b);
	evbuffer_free(b);
}


static void handle_mget(tcp_client* c, struct evbuffer* buffer) {
	int i, n, ksize;
	char k[MAX_TRANSACTION_SIZE];
	val* values[64];
	struct evbuffer* b = evbuffer_copy(buffer);

	for (i = 0; i < 64; i++) values[i] = NULL;
	transaction_set_get_cb(c->t, on_mget, c);
	
	evbuffer_remove(b, &n, sizeof(int));
	for (i = 0; i < n; i++) {
		evbuffer_remove(b, &ksize, sizeof(int));
		evbuffer_remove(b, k, ksize);
		values[i] = execute_get(c->t, k, ksize);
		if (values[i] == NULL)
			goto cleanup;
	}

	//	assert(transaction_read_only(c->t));
	int rv = transaction_remote_count(c->t);
	send_mget_result(c, rv, n, values);
	//transaction_clear(c->t);
	evbuffer_drain(buffer, evbuffer_get_length(buffer));

cleanup:
	evbuffer_free(b);
	for (i = 0; i < 64; i++)
		if (values[i] != NULL)
			val_free(values[i]);
}


static void handle_mput(tcp_client* c, struct evbuffer* b) {
	int rv, i, n;
	int ksize, vsize;
	char k[MAX_TRANSACTION_SIZE];
	char v[MAX_TRANSACTION_SIZE];
	
	evbuffer_remove(b, &n, sizeof(int));

	for (i = 0; i < n; i++) {
		evbuffer_remove(b, &ksize, sizeof(int));
		evbuffer_remove(b, k, ksize);
		evbuffer_remove(b, &vsize, sizeof(int));
		evbuffer_remove(b, v, vsize);
		execute_put(c->t, k, ksize, v, vsize);
	}
	
	rv = transaction_commit(c->t, c->id, on_commit);
	if (rv < 0) {
		transaction_clear(c->t);
		send_result(c->buffer_ev, -1);
		return;
	}
	evtimer_add(&c->timeout_ev, &commit_timeout);
}


static void on_mget_put(key* k, val* v, void* arg) {
	tcp_client* c = (tcp_client*)arg;
	handle_mget_put(c, bufferevent_get_input(c->buffer_ev));
}


static void handle_mget_put(tcp_client* c, struct evbuffer* buffer) {
	int rv, i, n;
	int ksize, vsize;
	char k[MAX_TRANSACTION_SIZE];
	char v[MAX_TRANSACTION_SIZE];
	val* tmp;
	struct evbuffer* b = evbuffer_copy(buffer);
	transaction_set_get_cb(c->t, on_mget_put, c);

	evbuffer_remove(b, &n, sizeof(int));
	for (i = 0; i < n; i++) {
		evbuffer_remove(b, &ksize, sizeof(int));
		evbuffer_remove(b, k, ksize);
		evbuffer_remove(b, &vsize, sizeof(int));
		evbuffer_remove(b, v, vsize);
		tmp = execute_get(c->t, k, ksize);
		if (tmp == NULL) {
			evbuffer_free(b);
			return;
		}
		val_free(tmp);
		execute_put(c->t, k, ksize, v, vsize);
	}
	
	evbuffer_free(b);
	evbuffer_drain(buffer, evbuffer_get_length(buffer));
	
	rv = transaction_commit(c->t, c->id, on_commit);
	if (rv < 0) {
		transaction_clear(c->t);
		send_result(c->buffer_ev, -1);
		return;
	}
	evtimer_add(&c->timeout_ev, &commit_timeout);
}

static void send_result(struct bufferevent* bev, int res) {
	int size = 0;
	bufferevent_write(bev, &size, sizeof(int));
	bufferevent_write(bev, &res, sizeof(int));
}


static int tcp_client_next_id() {
	static int id = 0;
	return id++;
}


static tcp_client* tcp_client_new() {
	tcp_client* c;
	c = malloc(sizeof(tcp_client));
	if (c == NULL) return NULL;
	memset(c, 0, sizeof(tcp_client));
	c->id = tcp_client_next_id();
	c->t = transaction_new();
	evtimer_set(&c->timeout_ev, on_commit_timeout, c);
	return c;
}


static int tcp_client_remove(int cid) { 
	tcp_client* c;
	c = hashtable_remove(clients, &cid);
	if (c == NULL) return 0;
	tcp_client_free(c);
	return 1;
}


static void tcp_client_free(tcp_client* c) {
	transaction_destroy(c->t);
	bufferevent_free(c->buffer_ev);
	evtimer_del(&c->timeout_ev);
	free(c);
}


static int key_equal(void* k1, void* k2) {
	int* a = (int*)k1;
	int* b = (int*)k2;
	return (*a == *b);
}

static unsigned int hash_from_client_bpt_key(void* k) {
	return joat_hash(k, sizeof(int) + sizeof(uint16_t));
}


static unsigned int hash_from_key(void* k) {
	return joat_hash(k, sizeof(int));
}


static int setnonblock(int fd) {
	int flags;
	flags = fcntl(fd, F_GETFL);
	if (flags < 0)
		return flags;
	flags |= O_NONBLOCK;
	if (fcntl(fd, F_SETFL, flags) < 0)
		return -1;
	return 0;
}


/***************************************************************************/
/*																		   */
/* New B+Tree stuff; perhaps put in new file 							   */
/*																		   */
/***************************************************************************/

static void on_bptree_initialize_bpt_session_no_commit(key* k, val* v, void* arg) {
	tcp_client* c = (tcp_client*)arg;
	handle_bptree_initialize_bpt_session_no_commit(c,
			bufferevent_get_input(c->buffer_ev));
}
static void on_bptree_initialize_bpt_session(key* k, val* v, void* arg) {
	tcp_client* c = (tcp_client*)arg;
	handle_bptree_initialize_bpt_session(c,
			bufferevent_get_input(c->buffer_ev));
}
static void on_bptree_insert(key* k, val* v, void* arg) {
	tcp_client* c = (tcp_client*)arg;
	handle_bptree_insert(c, bufferevent_get_input(c->buffer_ev));
}

static void on_bptree_delete(key* k, val* v, void* arg) {
	tcp_client* c = (tcp_client*)arg;
	handle_bptree_delete(c, bufferevent_get_input(c->buffer_ev));
}

static void on_bptree_update(key* k, val* v, void* arg) {
	tcp_client* c = (tcp_client*)arg;
	handle_bptree_update(c, bufferevent_get_input(c->buffer_ev));
}

static void on_bptree_search(key* k, val* v, void* arg) {
	tcp_client* c = (tcp_client*)arg;
	handle_bptree_search(c, bufferevent_get_input(c->buffer_ev));
}

static void on_bptree_index_first(key* k, val* v, void* arg) {
	tcp_client* c = (tcp_client*)arg;
	handle_bptree_index_first(c, bufferevent_get_input(c->buffer_ev));
}

static void on_bptree_debug(key* k, val* v, void* arg) {
	tcp_client* c = (tcp_client*)arg;
	handle_bptree_debug(c, bufferevent_get_input(c->buffer_ev));
}
static void on_bptree_index_first_no_key(key* k, val* v, void* arg) {
	tcp_client* c = (tcp_client*)arg;
	handle_bptree_index_first_no_key(c, bufferevent_get_input(c->buffer_ev));
}

static void on_bptree_index_next(key* k, val* v, void* arg) {
	tcp_client* c = (tcp_client*)arg;
	handle_bptree_index_next(c, bufferevent_get_input(c->buffer_ev));
}

static void on_bptree_index_next_mget(key* k, val* v, void* arg) {
	tcp_client* c = (tcp_client*)arg;
	handle_bptree_index_next_mget(c, bufferevent_get_input(c->buffer_ev));
}

/**
 * @brief
 * Handle the creation of a B+Tree session for this Tapioca connection without
 * destroying anything and providing the execution id
 *
 * @param[in] bpt_id
 * @param[in] open_flags
 * @param[in] execution_id
 * @param[out] rv - 0 if session already created; -1 on error ; otherwise
 	 	 	 	 bpt_id is returned
 */
static void handle_bptree_initialize_bpt_session_no_commit(tcp_client* c,
		struct evbuffer* buffer)
{
	int rv;
	client_bpt_id *c_b = malloc(sizeof(client_bpt_id));
	bptree_session *bps = malloc(sizeof(bptree_session));
	memset(bps, 0,sizeof(bptree_session));
	uint16_t bpt_id;
	enum bptree_open_flags open_flags;
	enum bptree_insert_flags insert_flags;
	uint32_t execution_id;

	struct evbuffer* b = evbuffer_copy(buffer);
	transaction_set_get_cb(c->t, on_bptree_initialize_bpt_session_no_commit, c);
	if (bptree_message_incomplete(b)) return;

	c_b->id = c->id;
	evbuffer_remove(b,&bpt_id, sizeof(uint16_t));
	evbuffer_remove(b,&open_flags, sizeof(enum bptree_open_flags));
	evbuffer_remove(b,&insert_flags, sizeof(enum bptree_insert_flags));
	evbuffer_remove(b,&execution_id, sizeof(uint32_t));

	c_b->bpt_id = bpt_id;

	bps->bpt_id = bpt_id;
	bps->execution_id = execution_id;
	bps->tapioca_client_id = c->id;
	bps->insert_count = 0;
	bps->t = c->t;
	bps->cursor_node = NULL;

	rv = bptree_initialize_bpt_session_no_commit(
			bps, bpt_id, open_flags, insert_flags, execution_id);
	if (rv == BPTREE_OP_TAPIOCA_NOT_READY) return;
	if (rv == BPTREE_OP_SUCCESS)
	{
		bptree_session *bps2;
//		assert(hashtable_search(client_bpt_ids, c_b) == NULL);
		bps2 = hashtable_search(client_bpt_ids, c_b);
		if (bps2 != NULL) free(bps2);
		assert(hashtable_insert(client_bpt_ids, c_b, bps));
		printf("Session_nocomm: %d:%d\n", c_b->id, c_b->bpt_id);
		fflush(stdout);
	}

	evbuffer_free(b);
	evbuffer_drain(buffer, evbuffer_get_length(buffer));
	send_result(c->buffer_ev, rv);
}

/**
 * @brief
 * Create a B+Tree session for this Tapioca connection
 * Does an implicit commit!
 *
 * @param[in] bpt_id
 * @param[in] open_flags
 * @param[out] rv - -1 on error ; otherwise bpt_id is returned
 */
static void handle_bptree_initialize_bpt_session(tcp_client* c,
		struct evbuffer* buffer)
{
	int rv;
	client_bpt_id *c_b = malloc(sizeof(client_bpt_id));
	bptree_session *bps = malloc(sizeof(bptree_session));
	uint16_t bpt_id;
	enum bptree_open_flags open_flags;
	enum bptree_insert_flags insert_flags;

	struct evbuffer* b = evbuffer_copy(buffer);
	transaction_set_get_cb(c->t, on_bptree_initialize_bpt_session, c);
	if (bptree_message_incomplete(b)) return;

	c_b->id = c->id;
	evbuffer_remove(b,&bpt_id, sizeof(uint16_t));
	evbuffer_remove(b,&open_flags, sizeof(enum bptree_open_flags));
	evbuffer_remove(b,&insert_flags, sizeof(enum bptree_insert_flags));

	c_b->bpt_id = bpt_id;

	bps->bpt_id = bpt_id;
	bps->tapioca_client_id = c->id;
	bps->t = c->t;

	rv = bptree_initialize_bpt_session(bps, bpt_id, open_flags, insert_flags);
	if (rv == BPTREE_OP_TAPIOCA_NOT_READY) return;
	if (rv == BPTREE_OP_SUCCESS)
	{
//		assert(hashtable_search(client_bpt_ids, c_b) == NULL);
		bptree_session *bps2;
		bps2 = hashtable_search(client_bpt_ids, c_b);
		// If we had the same session before remove the old one
		if(bps2 != NULL) free(bps2);
		assert(hashtable_insert(client_bpt_ids, c_b, bps));
		printf("Session: %d:%d\n", c_b->id, c_b->bpt_id);
		fflush(stdout);
	}

	evbuffer_free(b);
	evbuffer_drain(buffer, evbuffer_get_length(buffer));
	send_result(c->buffer_ev, rv);
}

/**
 * Manages the 'header' of all calls made to B+Tree operations and prepares
 * the bptree session
 */
static bptree_session *
retrieve_bptree_session(tcp_client* c, struct evbuffer *buffer)
{
	bptree_session *bps;
	client_bpt_id c_b;
	c_b.id = c->id;
	// FIXME In some cases this is failing, we are not ensuring that
	// libevent has already sent out bpt info!
	assert(evbuffer_get_length(buffer) >= sizeof(uint16_t));
	evbuffer_remove(buffer,&c_b.bpt_id, sizeof(uint16_t));
	bps = hashtable_search(client_bpt_ids, &c_b);
	if(bps != NULL)
		assert(bps->bpt_id == c_b.bpt_id && bps->tapioca_client_id == c_b.id);
	if (bps == NULL) {
		printf("Failed to find session %d:%d!\n", c_b.id, c_b.bpt_id);
	}
	assert(bps != NULL);
	return bps;
}

static void handle_bptree_set_num_fields(tcp_client* c,struct evbuffer* buffer)
{
	int rv;
	bptree_session *bps;
	int16_t num_fields;

	struct evbuffer* b = evbuffer_copy(buffer);
	bps = retrieve_bptree_session(c, b);
	if (bps == NULL)
	{
		printf("Couldn't find bptree_session in set_num_fields!\n");
		rv = -1;
	} else
	{
		evbuffer_remove(b,&num_fields, sizeof(int16_t));
		rv = bptree_set_num_fields(bps,num_fields);
	}

	evbuffer_free(b);
	evbuffer_drain(buffer, evbuffer_get_length(buffer));
	send_result(c->buffer_ev, rv);
}

static void handle_bptree_set_field_info(tcp_client* c, struct evbuffer* buffer)
{
	int rv;
	bptree_session *bps;
	int16_t field_num;
	int16_t field_sz;
	enum bptree_field_comparator field_comp;

	struct evbuffer* b = evbuffer_copy(buffer);
	bps = retrieve_bptree_session(c,b);
	if (bps == NULL)
	{
		printf("Couldn't find bptree_session in set field info!\n");
		rv = -1;
	} else
	{
		evbuffer_remove(b,&field_num, sizeof(int16_t));
		evbuffer_remove(b,&field_sz, sizeof(int16_t));
		evbuffer_remove(b,&field_comp, sizeof(enum bptree_field_comparator));

		rv = bptree_set_field_info(bps, field_num,field_sz, field_comp,
				comparators[field_comp]);
	}
	evbuffer_free(b);
	evbuffer_drain(buffer, evbuffer_get_length(buffer));
	send_result(c->buffer_ev, rv);
}

static void handle_bptree_insert(tcp_client* c,struct evbuffer* buffer)
{
	int rv;
	bptree_session *bps;
	int32_t ksize, vsize;
	char k[MAX_TRANSACTION_SIZE];
	char v[MAX_TRANSACTION_SIZE];
	bzero(k,MAX_TRANSACTION_SIZE);
	bzero(v,MAX_TRANSACTION_SIZE);
	enum bptree_insert_flags insert_flags;

	struct evbuffer* b = evbuffer_copy(buffer);
	transaction_set_get_cb(c->t, on_bptree_insert, c);
	if (bptree_message_incomplete(b)) return;

	bps = retrieve_bptree_session(c,b);
	if (bps ==  NULL)
	{
		printf("Big trouble yo!\n");
		fflush(stdout);
		rv = -1;
		evbuffer_drain(b,evbuffer_get_length(b));
		assert(1 == 0xBEEFCAFE);
	}
	else
	{
		evbuffer_remove(b,&ksize, sizeof(int32_t));
		evbuffer_remove(b,k, ksize);
		evbuffer_remove(b,&vsize, sizeof(int32_t));
		evbuffer_remove(b,v, vsize);
		// TODO We've moved this out of insert; clean it up elsewhere
		//evbuffer_remove(b,&insert_flags, sizeof(enum bptree_insert_flags));

		rv = bptree_insert(bps,k,ksize,v,vsize);//,insert_flags);

		evbuffer_free(b);
		if (rv == BPTREE_OP_TAPIOCA_NOT_READY) return;
		if (rv == BPTREE_OP_RETRY_NEEDED) {
			printf("Insert failed due to unavailable key, txn retry needed, bpt %d\n",bps->bpt_id);
		}
		evbuffer_drain(buffer, evbuffer_get_length(buffer));
		c->write_count++;
		send_result(c->buffer_ev, rv);

#ifdef TRACE_MODE
		c->kv_prev.k = malloc(ksize);
		c->kv_prev.v = malloc(vsize);
		memcpy(c->kv_prev.k, k, ksize);
		memcpy(c->kv_prev.v, v, vsize);
		c->kv_prev.ksize = ksize;
		c->kv_prev.vsize = vsize;
#endif

	}

}

static void handle_bptree_update(tcp_client* c,struct evbuffer* buffer)
{
	int rv;
	bptree_session *bps;
	int32_t ksize, vsize;
	char k[MAX_TRANSACTION_SIZE];
	char v[MAX_TRANSACTION_SIZE];
	bzero(k,MAX_TRANSACTION_SIZE);
	bzero(v,MAX_TRANSACTION_SIZE);

	struct evbuffer* b = evbuffer_copy(buffer);
	transaction_set_get_cb(c->t, on_bptree_update, c);
	if (bptree_message_incomplete(b)) return;

	bps = retrieve_bptree_session(c,b);
	evbuffer_remove(b,&ksize, sizeof(int32_t));
	evbuffer_remove(b,k, ksize);
	evbuffer_remove(b,&vsize, sizeof(int32_t));
	evbuffer_remove(b,v, vsize);

	rv = bptree_update(bps,k,ksize,v,vsize);
	if (rv == BPTREE_OP_TAPIOCA_NOT_READY) return;

	evbuffer_free(b);
	evbuffer_drain(buffer, evbuffer_get_length(buffer));
	send_result(c->buffer_ev, rv);
}

static void handle_bptree_delete(tcp_client* c,struct evbuffer* buffer)
{
	int rv;
	bptree_session *bps;
	int32_t ksize, vsize;
	char k[MAX_TRANSACTION_SIZE];
	char v[MAX_TRANSACTION_SIZE];
	bzero(k,MAX_TRANSACTION_SIZE);
	bzero(v,MAX_TRANSACTION_SIZE);

	struct evbuffer* b = evbuffer_copy(buffer);
	transaction_set_get_cb(c->t, on_bptree_delete, c);
	if (bptree_message_incomplete(b)) return;

	bps = retrieve_bptree_session(c,b);
	evbuffer_remove(b,&ksize, sizeof(int32_t));
	evbuffer_remove(b,k, ksize);
	evbuffer_remove(b,&vsize, sizeof(int32_t));
	evbuffer_remove(b,v, vsize);

	rv = bptree_delete(bps,k,ksize,v,vsize);
	if (rv == BPTREE_OP_TAPIOCA_NOT_READY) return;

	evbuffer_free(b);
	evbuffer_drain(buffer, evbuffer_get_length(buffer));
	send_result(c->buffer_ev, rv);
}

static void send_multi_cursor_result(tcp_client *c,
		bptree_mget_result *bmres, int rv, int16_t rows, int buf_sz)
{
	// buf_sz will be the size of the key/value buffers but we need to
	// add the size of the k/vsizes and the variable holding the num of rows
	bptree_mget_result *cur = bmres;
	int i, sz = 0; // we'll verify anyway that the size we were given is right
	// FIXME Something is not adding up correctly here....
	int size = sizeof(int32_t)*2*rows + sizeof(int16_t) + buf_sz;
	bufferevent_write(c->buffer_ev, &size,sizeof(int));
	bufferevent_write(c->buffer_ev, &rv,sizeof(int));
	bufferevent_write(c->buffer_ev, &rows,sizeof(int16_t));
	sz += sizeof(int16_t);
	for (i=1; i<=rows; i++)
	{
		bufferevent_write(c->buffer_ev, &cur->ksize,sizeof(int32_t));
		bufferevent_write(c->buffer_ev,cur->k,cur->ksize);
		bufferevent_write(c->buffer_ev, &cur->vsize,sizeof(int32_t));
		bufferevent_write(c->buffer_ev,cur->v,cur->vsize);
		if(i < rows) cur = cur->next;
		sz += sizeof(int32_t)*2 + cur->ksize + cur->vsize;

	}
	assert(sz == size);
	assert(cur->next == NULL);
}

static void send_cursor_result(tcp_client *c, int rv,
		int32_t ksize, void *k, int32_t vsize, void *v)
{
	int size = sizeof(int32_t) + vsize + sizeof(int32_t) + ksize;
	bufferevent_write(c->buffer_ev, &size,sizeof(int));
	bufferevent_write(c->buffer_ev, &rv,sizeof(int));
	bufferevent_write(c->buffer_ev, &ksize,sizeof(int32_t));
	bufferevent_write(c->buffer_ev,k,ksize);
	bufferevent_write(c->buffer_ev, &vsize,sizeof(int32_t));
	bufferevent_write(c->buffer_ev,v,vsize);
}

static void send_bptree_result(tcp_client *c, int rv, int32_t vsize, void *v)
{
	int size = sizeof(int32_t) + vsize;
	bufferevent_write(c->buffer_ev, &size,sizeof(int));
	bufferevent_write(c->buffer_ev, &rv,sizeof(int));
	bufferevent_write(c->buffer_ev, &vsize,sizeof(int32_t));
	bufferevent_write(c->buffer_ev,v,vsize);
}

static void handle_bptree_search(tcp_client* c,struct evbuffer* buffer)
{
	int rv;
	bptree_session *bps;
	int32_t ksize, vsize;
	char k[MAX_TRANSACTION_SIZE];
	char v[MAX_TRANSACTION_SIZE];
	// FIXME This is a bit of overkill, and slows things down, but
	// bptree_search assumes that any bytes not written to are 0 (problematic
	// for comparison of strings if there is lingering data
	bzero(k,MAX_TRANSACTION_SIZE);
	bzero(v,MAX_TRANSACTION_SIZE);

	struct evbuffer* b = evbuffer_copy(buffer);
	transaction_set_get_cb(c->t, on_bptree_search, c);
	if (bptree_message_incomplete(b)) return;

	bps = retrieve_bptree_session(c,b);
	if(bps == NULL)
	{
		printf("Couldn't find bptree_session in bptree_search!\n");
		send_bptree_result(c,-1,0,v);
	}
	else {

		evbuffer_remove(b,&ksize, sizeof(int32_t));
		evbuffer_remove(b,k, ksize);

		rv = bptree_search(bps,k,ksize,v,&vsize);

		evbuffer_free(b);
		
		if (rv == BPTREE_OP_TAPIOCA_NOT_READY) return;

		evbuffer_drain(buffer, evbuffer_get_length(buffer));
		send_bptree_result(c,rv,vsize,(void *)v);
	}

}

static void handle_bptree_index_first(tcp_client* c,struct evbuffer* buffer)
{
	int rv;
	bptree_session *bps;
	int32_t ksize, vsize;
	char k[MAX_TRANSACTION_SIZE];
	char v[MAX_TRANSACTION_SIZE];
	bzero(k,MAX_TRANSACTION_SIZE);
	bzero(v,MAX_TRANSACTION_SIZE);

	struct evbuffer* b = evbuffer_copy(buffer);
	transaction_set_get_cb(c->t, on_bptree_index_first, c);
	if (bptree_message_incomplete(b)) return;

	bps = retrieve_bptree_session(c,b);
	if (bps == NULL)
	{
		printf("Couldn't find bptree_session in bptree_index_first!\n");
		send_cursor_result(c,-1,0,k,0,v);
	}
	else
	{
		rv = bptree_index_first(bps,k,&ksize,v,&vsize);
		if (rv == BPTREE_OP_TAPIOCA_NOT_READY) return;

		evbuffer_free(b);
		evbuffer_drain(buffer, evbuffer_get_length(buffer));

		send_cursor_result(c,rv,ksize, (void*) k, vsize,(void *)v);
	}
}

static void handle_bptree_index_next(tcp_client* c,struct evbuffer* buffer)
{
	int rv;
	bptree_session *bps;
	int32_t ksize, vsize;
	char k[MAX_TRANSACTION_SIZE];
	char v[MAX_TRANSACTION_SIZE];
	bzero(k,MAX_TRANSACTION_SIZE);
	bzero(v,MAX_TRANSACTION_SIZE);

	struct evbuffer* b = evbuffer_copy(buffer);
	transaction_set_get_cb(c->t, on_bptree_index_next, c);
	if (bptree_message_incomplete(b)) return;

	bps = retrieve_bptree_session(c,b);
	if (bps == NULL)
	{
		printf("Couldn't find bptree_session in bptree_index_next!\n");
		send_cursor_result(c,-1,0,k,0,v);
	}
	else
	{
		rv = bptree_index_next(bps,k,&ksize,v,&vsize);
		if (rv == BPTREE_OP_TAPIOCA_NOT_READY) return;

		evbuffer_free(b);
		evbuffer_drain(buffer, evbuffer_get_length(buffer));

		send_cursor_result(c,rv,ksize, (void*) k, vsize,(void *)v);
	}

}

static void handle_bptree_index_next_mget(tcp_client* c,struct evbuffer* buffer)
{
	int rv;
	bptree_session *bps;
	int16_t rows;
	int buf_sz;
	char k[MAX_TRANSACTION_SIZE];
	char v[MAX_TRANSACTION_SIZE];
	bzero(k,MAX_TRANSACTION_SIZE);
	bzero(v,MAX_TRANSACTION_SIZE);

	struct evbuffer* b = evbuffer_copy(buffer);
	transaction_set_get_cb(c->t, on_bptree_index_next_mget, c);
	if (bptree_message_incomplete(b)) return;

	bps = retrieve_bptree_session(c,b);
	if (bps == NULL)
	{
		printf("Couldn't find bptree_session in bptree_index_next_mget!\n");
		send_cursor_result(c,-1,0,k,0,v);
	}
	else
	{
		bptree_mget_result *bmres;
		rows = 9;
		rv = bptree_index_next_mget(bps,&bmres, &rows, &buf_sz);
		if (rv == BPTREE_OP_TAPIOCA_NOT_READY) return;

		evbuffer_free(b);
		evbuffer_drain(buffer, evbuffer_get_length(buffer));

		send_multi_cursor_result(c, bmres, rv, rows, buf_sz);
		bptree_mget_result_free(&bmres);
	}
}

static void handle_bptree_index_first_no_key(tcp_client* c,struct evbuffer* buffer)
{
	int rv;
	bptree_session *bps;

	struct evbuffer* b = evbuffer_copy(buffer);
	transaction_set_get_cb(c->t, on_bptree_index_first_no_key, c);
	if (bptree_message_incomplete(b)) return;

	bps = retrieve_bptree_session(c,b);
	if (bps == NULL)
	{
		printf("Couldn't find bptree_session in bptree_idx_first_no_key!\n");
		rv = -1;
		evbuffer_drain(b,evbuffer_get_length(b));
	}
	else
	{
		rv = bptree_index_first_no_key(bps);
		if (rv == BPTREE_OP_TAPIOCA_NOT_READY) return;
	}
	evbuffer_free(b);
	evbuffer_drain(buffer, evbuffer_get_length(buffer));
	send_result(c->buffer_ev, rv);
}

/*@ To avoid having to create all the overhead for a bunch of debug functions
 * just roll everything into one debug method and provide an enum to select
 * which one to do
 */
static void handle_bptree_debug(tcp_client* c, struct evbuffer* buffer)
{
	int rv;
	bptree_session *bps;
	int16_t field_num;
	int16_t field_sz;
	enum bptree_debug_option debug_opt;
	static uuid_t failed_node;

	struct evbuffer* b = evbuffer_copy(buffer);
	transaction_set_get_cb(c->t, on_bptree_debug, c);
	if (bptree_message_incomplete(b)) return;
	
	bps = retrieve_bptree_session(c,b);
	if (bps == NULL)
	{
		printf("Couldn't find bptree_session in bptree_debug!\n");
		rv = -1;
	} else
	{
		evbuffer_remove(b,&debug_opt, sizeof(enum bptree_debug_option));
		rv = bptree_debug(bps, debug_opt, failed_node);
		if (rv == BPTREE_OP_TAPIOCA_NOT_READY) return;
	}
	uuid_clear(failed_node);
	evbuffer_free(b);
	evbuffer_drain(buffer, evbuffer_get_length(buffer));
	send_result(c->buffer_ev, rv);
}
