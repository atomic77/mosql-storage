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
int NodeID = 0;

struct evbuffer* rbuffer;

static void http_open_cb(struct evhttp_request* r, void* arg);
static void http_close_cb(struct evhttp_request* r, void* arg);
static void http_put_cb(struct evhttp_request* r, void* arg);
static void http_get_cb(struct evhttp_request* r, void* arg);
static void http_commit_cb(struct evhttp_request* r, void* arg);
static void http_reply_wrong_params(struct evhttp_request* r);
static void http_reply_commit_result(struct evhttp_request* r, int res);
static int expect_bytes(struct evbuffer* b, void* p, int bytes);


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
	
	rbuffer = evbuffer_new();
		
	return 0;
}


static void http_open_cb(struct evhttp_request* r, void* arg) {
	static int id = 0;
	DEBUG_ENTER();

	id++;
	
	evbuffer_add(rbuffer, &id, sizeof(int));
	evbuffer_add(rbuffer, &NodeID, sizeof(int));
	evhttp_send_reply(r, HTTP_OK, "OK", rbuffer);
}


static void http_close_cb(struct evhttp_request* r, void* arg) {
	int cid;
	
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
	evhttp_send_reply(r, HTTP_OK, "OK", NULL);
}


static void http_put_cb(struct evhttp_request* r, void* arg) {
	int rv;
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
	
	evhttp_send_reply(r, HTTP_OK, "OK", NULL);
}


static void http_get_cb(struct evhttp_request* r, void* arg) {
	int rv;
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

	evbuffer_add(rbuffer, k, ksize);
	evhttp_send_reply(r, HTTP_OK, "OK", rbuffer);
}


static void http_commit_cb(struct evhttp_request* r, void* arg) {
	int rv, cid;

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

	http_reply_commit_result(r, 1);
}


static void http_reply_wrong_params(struct evhttp_request* r) {
	DEBUG_ENTER();
	evhttp_send_reply(r, HTTP_NOTFOUND, "NOT FOUND", NULL);
}


static void http_reply_commit_result(struct evhttp_request* r, int res) {
	DEBUG_ENTER();
	evbuffer_add(rbuffer, &res, sizeof(int));
	evhttp_send_reply(r, HTTP_OK, "OK", rbuffer);
}


static int expect_bytes(struct evbuffer* b, void* p, int bytes) {
	if (EVBUFFER_LENGTH(b) < bytes)
		return 0;
	evbuffer_remove(b, p, bytes);
	return 1;
}
