#include <stdlib.h>
#include <sys/queue.h>
#include <event.h>
#include <evhttp.h>
#include <string.h>

#include "http.h"
#include "hash.h"
#include "hashtable.h"
#include "transaction.h"


#ifdef DEBUG
	#define DEBUG_ENTER() printf("%s\n", __func__)
#else
	#define DEBUG_ENTER()
#endif


typedef struct http_client_t {
	int id;
	transaction* t;
	struct event timeout_ev;
	struct evhttp_request* r;
} http_client;


static struct hashtable* clients;
static struct timeval commit_timeout = {0, 500000};


static void http_open_cb(struct evhttp_request* r, void* arg);
static void http_close_cb(struct evhttp_request* r, void* arg);
static void http_put_cb(struct evhttp_request* r, void* arg);
static void http_get_cb(struct evhttp_request* r, void* arg);
static void http_commit_cb(struct evhttp_request* r, void* arg);
static void http_reply_wrong_params(struct evhttp_request* r);
static void http_reply_commit_result(struct evhttp_request* r, int res);
static http_client* http_client_new();
static void http_client_free(http_client* c);
static int http_client_remove(int cid);
static int expect_bytes(struct evbuffer* b, void* p, int bytes);
static int key_equal(void* k1, void* k2);
static unsigned int hash_from_key(void* k);


int http_init() {
	int rv = 0;
	struct evhttp* http;
	
	http = evhttp_new(NULL);
	if (http == NULL) return -1;
	
	rv = evhttp_bind_socket(http, "0.0.0.0", 8080);
	if (rv == -1) return -1;
	
	evhttp_set_cb(http, "/open", http_open_cb, NULL);
	evhttp_set_cb(http, "/close", http_close_cb, NULL);
	evhttp_set_cb(http, "/put", http_put_cb, NULL);
	evhttp_set_cb(http, "/get", http_get_cb, NULL);
	evhttp_set_cb(http, "/commit", http_commit_cb, NULL);
	
	clients = create_hashtable(256, hash_from_key, key_equal, free);
	if (clients == NULL) return -1;
	
	return 0;
}


static void http_open_cb(struct evhttp_request* r, void* arg) {
	int rv, *k;
	http_client* c;
	struct evbuffer* b;
	
	DEBUG_ENTER();
	
	c = http_client_new();
	k = malloc(sizeof(int));
	*k = c->id;
	rv = hashtable_insert(clients, k, c);
	
	b = evbuffer_new();
	evbuffer_add(b, &c->id, sizeof(int));
	evbuffer_add(b, &NodeID, sizeof(int));
	evhttp_send_reply(r, HTTP_OK, "OK", b);
	evbuffer_free(b);
}


static void http_close_cb(struct evhttp_request* r, void* arg) {
	int rv, cid;
	
	DEBUG_ENTER();
	
	if (r->type != EVHTTP_REQ_POST) {
		printf("not a post request!\n");
		http_reply_wrong_params(r);
		return;
	}
	
	if (EVBUFFER_LENGTH(r->input_buffer) != sizeof(int)) {
		printf("wrong number of bytes\n");
		http_reply_wrong_params(r);
		return;
	}
	
	evbuffer_remove(r->input_buffer, &cid, sizeof(int));
	rv = http_client_remove(cid);
	
	if (rv == 1) {
		evhttp_send_reply(r, HTTP_OK, "OK", NULL);
	} else {
		printf("no such client %d\n", cid);
		http_reply_wrong_params(r);
	}
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


static void http_put_cb(struct evhttp_request* r, void* arg) {
	int rv;
	http_client* c;
	int cid, ksize, vsize;
	char k[8192];
	char v[8192];
	
	DEBUG_ENTER();
	
	if (r->type != EVHTTP_REQ_POST) {
		printf("not a post request!\n");
		http_reply_wrong_params(r);
		return;
	}
	
	rv = expect_bytes(r->input_buffer, &cid, sizeof(int));
	if (rv == 0) {
		printf("expected client id\n");
		http_reply_wrong_params(r);
		return;
	}

	c = hashtable_search(clients, &cid);
	if (c == NULL) {
		printf("client %d not found\n", cid);
		http_reply_wrong_params(r);
		return;
	}
	
	rv = expect_bytes(r->input_buffer, &ksize, sizeof(int));
	if (rv == 0) {
		printf("expected key size\n");
		http_reply_wrong_params(r);
		return;
	}
	
	rv = expect_bytes(r->input_buffer, k, ksize);
	if (rv == 0) {
		printf("expected key (%d bytes)\n", ksize);
		http_reply_wrong_params(r);
		return;
	}
	
	rv = expect_bytes(r->input_buffer, &vsize, sizeof(int));
	if (rv == 0) {
		printf("expected value size\n");
		http_reply_wrong_params(r);
		return;
	}
	
	rv = expect_bytes(r->input_buffer, v, vsize);
	if (rv == 0) {
		printf("expected value (%d bytes)\n", vsize);
		http_reply_wrong_params(r);
		return;
	}
	
	execute_put(c->t, k, ksize, v, vsize);
	evhttp_send_reply(r, HTTP_OK, "OK", NULL);
}


static void on_get(key* k, val* v, void* arg) {
	http_client* c;
	
	c = (http_client*)arg;

	if (v == NULL) {
		evhttp_send_reply(c->r, HTTP_OK, "OK", NULL);
	} else {
		struct evbuffer* b = NULL;
		b = evbuffer_new();
		evbuffer_add(b, v->data, v->size);
		evhttp_send_reply(c->r, HTTP_OK, "OK", b);
		evbuffer_free(b);
	}
}


static void execute_get(transaction* t, void* k, int ksize) {
	key _k;
	
	_k.data = k;
	_k.size = ksize;
 	transaction_get(t, &_k);
}


static void http_get_cb(struct evhttp_request* r, void* arg) {
	int rv;
	http_client* c;
	int cid, ksize;
	char k[8192];
	
	DEBUG_ENTER();
	
	if (r->type != EVHTTP_REQ_POST) {
		printf("not a post request!\n");
		http_reply_wrong_params(r);
		return;
	}
	
	rv = expect_bytes(r->input_buffer, &cid, sizeof(int));
	if (rv == 0) {
		printf("expected client id\n");
		http_reply_wrong_params(r);
		return;
	}
	
	c = hashtable_search(clients, &cid);
	if (c == NULL) {
		printf("client %d not found\n", cid);
		http_reply_wrong_params(r);
		return;
	}
	
	rv = expect_bytes(r->input_buffer, &ksize, sizeof(int));
	if (rv == 0) {
		printf("expected key size\n");
		http_reply_wrong_params(r);
		return;
	}
	
	rv = expect_bytes(r->input_buffer, k, ksize);
	if (rv == 0) {
		printf("expected key (%d bytes)\n", ksize);
		http_reply_wrong_params(r);
		return;
	}

	c->r = r;
	execute_get(c->t, k, ksize);
}


static void on_commit(tr_id* id, int commit_result) {
	http_client* c;

	DEBUG_ENTER();
	
	c = hashtable_search(clients, &id->client_id);
	if (c == NULL) {
		return;
	}
	// TODO  checking the sequence number here is not sufficient. 
	// Suppose a timeout occurs, the sequence number remains unchanged until
	// the next call to transaction commit...
	if (id->seqnumber != c->t->id.seqnumber) {
		return;
	}
	
	http_reply_commit_result(c->r, commit_result);
	transaction_clear(c->t);
	evtimer_del(&c->timeout_ev);
}


static void on_commit_timeout(int fd, short event, void* arg) {
	int* cid;
	http_client* c;
	cid = (int*)arg;
	
	DEBUG_ENTER();
	
	c = hashtable_search(clients, cid);
	if (c == NULL) {
		printf("On commit timeout: client not found\n");
		return;
	}
		
	transaction_clear(c->t);
	http_reply_commit_result(c->r, -1);
}


static void http_commit_cb(struct evhttp_request* r, void* arg) {
	int rv, cid;
	http_client* c;

	DEBUG_ENTER();

	if (r->type != EVHTTP_REQ_POST) {
		printf("not a post request!\n");
		http_reply_wrong_params(r);
		return;
	}
	
	rv = expect_bytes(r->input_buffer, &cid, sizeof(int));
	if (rv == 0) {
		printf("expected client id\n");
		http_reply_wrong_params(r);
		return;
	}
	
	c = hashtable_search(clients, &cid);
	if (c == NULL) {
		printf("client %d not found\n", cid);
		http_reply_wrong_params(r);
		return;
	}
	
	if (transaction_read_only(c->t)) {
		transaction_clear(c->t);
		http_reply_commit_result(r, 1);
		return;
	}
	
	c->r = r;
	rv = transaction_commit(c->t, c->id, on_commit);
	if (rv < 0) {
		transaction_clear(c->t);
		http_reply_commit_result(r, -1);
	}

	evtimer_add(&c->timeout_ev, &commit_timeout);
}


static void http_reply_wrong_params(struct evhttp_request* r) {
	DEBUG_ENTER();
	evhttp_send_reply(r, HTTP_NOTFOUND, "NOT FOUND", NULL);
}


static void http_reply_commit_result(struct evhttp_request* r, int res) {
	struct evbuffer* b;
	DEBUG_ENTER();
	b = evbuffer_new();
	evbuffer_add(b, &res, sizeof(int));
	evhttp_send_reply(r, HTTP_OK, "OK", b);
	evbuffer_free(b);
}


static int http_client_next_id() {
	static int id = 0;
	return id++;
}


static http_client* http_client_new() {
	http_client* c;
	c = malloc(sizeof(http_client));
	if (c == NULL) return NULL;
	c->id = http_client_next_id();
	c->t = transaction_new();
	transaction_set_get_cb(c->t, on_get, c);
	evtimer_set(&c->timeout_ev, on_commit_timeout, &c->id);
	return c;
}


static int http_client_remove(int cid) { 
	http_client* c;
	c = hashtable_remove(clients, &cid);
	if (c == NULL) return 0;
	http_client_free(c);
	return 1;
}


static void http_client_free(http_client* c) {
	transaction_destroy(c->t);
	free(c);
}


static int expect_bytes(struct evbuffer* b, void* p, int bytes) {
	if (EVBUFFER_LENGTH(b) < bytes)
		return 0;
	evbuffer_remove(b, p, bytes);
	return 1;
}


static int key_equal(void* k1, void* k2) {
	int* a = (int*)k1;
	int* b = (int*)k2;
	return (*a == *b);
}


static unsigned int hash_from_key(void* k) {
	return joat_hash(k, sizeof(int));
}
