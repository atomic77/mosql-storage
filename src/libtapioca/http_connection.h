#ifndef _HTTP_CONNECTION_H_
#define _HTTP_CONNECTION_H_

#include <event.h>
#include <evhttp.h>

typedef struct http_connection_t {
	struct event_base* base;
	struct evhttp_connection* c;
	struct evbuffer* b;
	int response_code;
} http_connection;

http_connection* http_connection_new(char* address, int port);
void http_connection_free(http_connection* h);
int http_connection_request(http_connection* hc, char* uri, struct evbuffer* body);
int http_connection_post_request(http_connection* hc, char* uri, struct evbuffer* post, struct evbuffer* body);

#endif
