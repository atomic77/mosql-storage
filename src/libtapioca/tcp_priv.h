#ifndef _TCP_PRIV_H_
#define _TCP_PRIV_H_

typedef struct tcp_handle {
	int node_id;
	int client_id;
	tcp_connection* c;
	struct evbuffer* b;
	int mget_buffer_count;
	struct evbuffer* mget_buffer;
	int mput_buffer_count;
	struct evbuffer* mput_buffer;
} tcp_handle;

#endif
