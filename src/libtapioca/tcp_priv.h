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

#define CHECK_BUFFER_SIZE(b, s) \
 	assert(evbuffer_get_length(msg) == (s+2*sizeof(int)))
 	
struct evbuffer* buffer_with_header(int size, int type);
void buffer_add_data_with_size(struct evbuffer* msg, void* data, int size);


#endif
