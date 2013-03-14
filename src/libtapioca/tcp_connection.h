#ifndef _TCP_CONNECTION_H_
#define _TCP_CONNECTION_H_

#include <event2/event.h>

typedef struct tcp_connection_t {
	int sock;
	short error;
	struct evbuffer* out;
	struct event_base* base;
	struct bufferevent* buffer_ev;
} tcp_connection;


tcp_connection* tcp_connection_new(const char* address, int port);
void tcp_connection_free(tcp_connection* c);
int tcp_write_buffer(tcp_connection* c, struct evbuffer* in, struct evbuffer* out);

#endif
