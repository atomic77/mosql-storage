#include "protocol.h"
#include "http_connection.h"
#include <stdlib.h>
#include <event.h>
#include <evhttp.h>

#ifdef USE_HTTP_PROTOCOL

typedef struct http_handle_t {
	int node_id;
	int client_id;
	http_connection* c;
	struct evbuffer* b;
} http_handle;


static http_handle*
http_handle_new(char* address, int port) {
	http_handle* h;
	h = malloc(sizeof(http_handle));
	h->client_id = -1;
	h->c = http_connection_new(address, port);
	h->b = evbuffer_new();
	return h;
}


static void
http_handle_free(http_handle* h) {
	evbuffer_free(h->b);
	http_connection_free(h->c);
	free(h);
}


void* 
protocol_open(char* address, int port) {
	int rv;
	http_handle* h;
	h = http_handle_new(address, port);
	if (h == NULL) return NULL;
	rv = http_connection_request(h->c, "/open", h->b);
	if (rv != HTTP_OK) {
		http_handle_free(h);
		return NULL;
	}
	evbuffer_remove(h->b, &(h->client_id), sizeof(int));
	evbuffer_remove(h->b, &(h->node_id), sizeof(int));
	return h;
}


int
protocol_node_id(void* p) {
	http_handle* h = (http_handle*)p;
	return h->node_id;
}


void 
protocol_close(void* p) {
	struct evbuffer* b;
	http_handle* h = (http_handle*)p;
	b = evbuffer_new();
	evbuffer_add(b, &h->client_id, sizeof(int));
	http_connection_post_request(h->c, "/close", b, h->b);
	evbuffer_free(b);
	http_handle_free(h);
}


int
protocol_get(void* p, void* k, int ksize, void* v, int vsize) {
	int rv, bsize;
	struct evbuffer* b;
	http_handle* h = (http_handle*)p;
	b = evbuffer_new();
	evbuffer_add(b, &h->client_id, sizeof(int));
	evbuffer_add(b, &ksize, sizeof(int));
	evbuffer_add(b, k, ksize);
	rv = http_connection_post_request(h->c, "/get", b, h->b);
	if (rv != HTTP_OK) return -1;
	
	bsize = EVBUFFER_LENGTH(h->b);
	if (vsize < bsize)
		return -1;
	
	evbuffer_remove(h->b, v, bsize);
	return bsize;
}


int
protocol_put(void* p, void* k, int ksize, void* v, int vsize) {
	int rv = 1;
	struct evbuffer* b;
	http_handle* h = (http_handle*)p;
	b = evbuffer_new();
	evbuffer_add(b, &h->client_id, sizeof(int));
	evbuffer_add(b, &ksize, sizeof(int));
	evbuffer_add(b, k, ksize);
	evbuffer_add(b, &vsize, sizeof(int));
	evbuffer_add(b, v, vsize);
	rv = http_connection_post_request(h->c, "/put", b, NULL);
	evbuffer_free(b);
	if (rv != HTTP_OK) rv = -1;
	return rv;
}


int
protocol_commit(void* p) {
	int rv;
	struct evbuffer* b;
	http_handle* h = (http_handle*)p;
	b = evbuffer_new();
	evbuffer_add(b, &h->client_id, sizeof(int));
	rv = http_connection_post_request(h->c, "/commit", b, h->b);
	if (rv != HTTP_OK) return -1;
	evbuffer_free(b);
	evbuffer_remove(h->b, &rv, sizeof(int));
	return rv;
}

#endif
