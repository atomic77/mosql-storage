#include "protocol.h"
#include "tcp_connection.h"
#include "mget_result.h"
#include <stdlib.h>
#include <string.h>
#include <event2/event.h>
#include <event2/buffer.h>
#include <assert.h>
#include "tcp_priv.h"


#define CHECK_BUFFER_SIZE(b, s) \
 	assert(evbuffer_get_length(msg) == (s+2*sizeof(int)))

static void get_bptree_result(tcp_handle *h, int *rv, int32_t *vsize, void *v);

static tcp_handle*
tcp_handle_new(const char* address, int port) {
	tcp_handle* h;
	h = malloc(sizeof(tcp_handle));
	if (h == NULL) return NULL;
	h->client_id = -1;
	h->c = tcp_connection_new(address, port);
	if (h->c == NULL) return NULL;
	h->b = evbuffer_new();
	h->mget_buffer_count = 0;
	h->mget_buffer = evbuffer_new();
	h->mput_buffer_count = 0;
	h->mput_buffer = evbuffer_new();
	return h;
}


static void
tcp_handle_free(tcp_handle* h) {
	evbuffer_free(h->b);
	tcp_connection_free(h->c);
	free(h);
}


static struct evbuffer*
buffer_with_header(int size, int type) {
    struct evbuffer* msg = evbuffer_new();
    evbuffer_expand(msg, size + 2*sizeof(int));
    evbuffer_add(msg, &size, sizeof(int));
    evbuffer_add(msg, &type, sizeof(int));
    return msg;
}


static void
buffer_add_data_with_size(struct evbuffer* msg, void* data, int size) {
    evbuffer_add(msg, &size, sizeof(int));
	evbuffer_add(msg, data, size);
}


void* 
protocol_open(const char* address, int port) {
	int rv;
	tcp_handle* h;
	int size = 0, rtype = 0;
	h = tcp_handle_new(address, port);
	if (h == NULL) return NULL;	
	struct evbuffer* msg = buffer_with_header(size, rtype);
	rv = tcp_write_buffer(h->c, msg, h->b);
	evbuffer_free(msg);
	if (rv != 0) {
		tcp_handle_free(h);
		return NULL;
	}
	evbuffer_remove(h->b, &rv, sizeof(int));
	evbuffer_remove(h->b, &(h->client_id), sizeof(int));
	evbuffer_remove(h->b, &(h->node_id), sizeof(int));
	return h;
}


int
protocol_client_id(void* p) {
	tcp_handle* h = (tcp_handle*)p;
	return h->client_id;
}

int
protocol_node_id(void* p) {
	tcp_handle* h = (tcp_handle*)p;
	return h->node_id;
}


void 
protocol_close(void* p) {
	int size = 0, rtype = 1;
	tcp_handle* h = (tcp_handle*)p;	
	struct evbuffer* msg = buffer_with_header(size, rtype);	
	tcp_write_buffer(h->c, msg, h->b);
	evbuffer_free(msg);
	tcp_handle_free(h);
}


int
protocol_get(void* p, void* k, int ksize, void* v, int vsize) {
	int rv, bsize = 0;
    int size = sizeof(int) + ksize;
    int rtype = 4;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
    buffer_add_data_with_size(msg, k, ksize);
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);
	evbuffer_free(msg);
	if (rv != 0) { return -1; }

	evbuffer_remove(h->b, &rv, sizeof(int));
	evbuffer_remove(h->b, &bsize, sizeof(int));
	if (vsize < bsize) {
		evbuffer_drain(h->b, bsize);
		return -1;
	}
	evbuffer_remove(h->b, v, bsize);
	return bsize;
}


int
protocol_put(void* p, void* k, int ksize, void* v, int vsize) {
	int rv;
    int size = 2*sizeof(int) + ksize + vsize;
    int rtype = 5;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
    buffer_add_data_with_size(msg, k, ksize);
    buffer_add_data_with_size(msg, v, vsize);
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);
	evbuffer_free(msg);
	if (rv != 0) { return -1; }
	evbuffer_remove(h->b, &rv, sizeof(int));
	return rv;
}


int
protocol_commit(void* p) {
	int rv, commit;
	int size = 0, rtype = 2;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);
	evbuffer_free(msg);
	if (rv < 0) { return -1; }
	
	evbuffer_remove(h->b, &commit, sizeof(int));
	return commit;
}


int
protocol_rollback(void* p) {
	int rv;
	int size = 0, rtype = 3;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);
	evbuffer_free(msg);
	if (rv < 0) { return -1; }
	evbuffer_remove(h->b, &rv, sizeof(int));
	return rv;
}


int
protocol_mget(void* p, void* k, int ksize) {
	tcp_handle* h = (tcp_handle*)p;
	h->mget_buffer_count++;
	buffer_add_data_with_size(h->mget_buffer, k, ksize);
	return 1;
}


mget_result*
protocol_mget_commit(void* p) {
	int rv;
	tcp_handle* h = (tcp_handle*)p;
    int	size = sizeof(int) + evbuffer_get_length(h->mget_buffer);
    int rtype = 6;
    mget_result* result;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	evbuffer_add(msg, &h->mget_buffer_count, sizeof(int));
	evbuffer_add_buffer(msg, h->mget_buffer);
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);
	evbuffer_free(msg);
	h->mget_buffer_count = 0;
	if (rv < 0) { return NULL; }

	evbuffer_remove(h->b, &rv, sizeof(int));
    if (rv < 0) { return NULL; }
    
    result = malloc(sizeof(mget_result));
    result->buffer = evbuffer_new();
	evbuffer_remove(h->b, &result->count, sizeof(int));
    evbuffer_add_buffer(result->buffer, h->b);
    
	return result;
}


int
protocol_mget_int(void* p, int n, int* keys, int* values) {
	int i, size;
    mget_result* res;
	
	for (i = 0; i < n; i++)
		protocol_mget(p, &keys[i], sizeof(int));
		
	res = protocol_mget_commit(p);
	if (res == NULL) return -1;

	for (i = 0; i < res->count; i++) {
	    evbuffer_remove(res->buffer, &size, sizeof(int));
		evbuffer_remove(res->buffer, &values[i], sizeof(int));
	}

    mget_result_free(res);
    
	return 1;
}


int
protocol_mput_int(void* p, int n, int* keys, int* values) {
	int i;
	for (i = 0; i < n; i++)
		protocol_mput(p, &keys[i], sizeof(int), &values[i], sizeof(int));
	return protocol_mput_commit(p);
}


int
protocol_mput(void* p, void* k, int ksize, void* v, int vsize) {
	tcp_handle* h = (tcp_handle*)p;
	h->mput_buffer_count++;
    buffer_add_data_with_size(h->mput_buffer, k, ksize);
    buffer_add_data_with_size(h->mput_buffer, v, vsize);
	return evbuffer_get_length(h->mput_buffer);
}


// int
// protocol_mput_commit(void* p) {
// 	int rv;
// 	tcp_handle* h = (tcp_handle*)p;
// 	int	size = sizeof(int) + evbuffer_get_length(h->mput_buffer);
// 	int rtype = 7;
//     struct evbuffer* msg = buffer_with_header(size, rtype);
// 	evbuffer_add(msg, &h->mput_buffer_count, sizeof(int));
// 	evbuffer_add_buffer(msg, h->mput_buffer);
// 	CHECK_BUFFER_SIZE(msg, size);
// 	rv = tcp_write_buffer(h->c, msg, h->b);
// 	evbuffer_free(msg);
// 	if (rv != 0) { return -1; }
// 	h->mput_buffer_count = 0;
// 	evbuffer_remove(h->b, &rv, sizeof(int));
// 	return rv;
// }


int
do_mput_commit(tcp_handle* h, char* data, int datasize) {
	int rv, rtype = 7;
 	int size = sizeof(int) + datasize;
    struct evbuffer* msg = buffer_with_header(size, rtype);

	evbuffer_add(msg, &h->mput_buffer_count, sizeof(int));
	evbuffer_add(msg, data, datasize);
	CHECK_BUFFER_SIZE(msg, size);
	
	rv = tcp_write_buffer(h->c, msg, h->b);
	evbuffer_free(msg);
	if (rv != 0) { return -1; }
	evbuffer_remove(h->b, &rv, sizeof(int));
	return rv;
}


int
protocol_mput_commit_retry(void* p, int times) {
	int rv = -1;
	int notsorandom = 0;
	time_t tm;
	char buf[64*1024];
	tcp_handle* h = (tcp_handle*)p;
	int size = evbuffer_remove(h->mput_buffer, buf, 64*1024);
	while (times > 0 && rv < 0) {
		rv = do_mput_commit(h, buf, size);
		times--;
		time(&tm);
		notsorandom = (tm * protocol_client_id(p)) % (100);
		if (rv < 0)	usleep(50 * 1000 + notsorandom*1000);

	}
	h->mput_buffer_count = 0;
	return rv;
}


int
protocol_mput_commit(void* p) {
	return protocol_mput_commit_retry(p, 1);
}


int
protocol_mget_put(void* p, void* k, int ksize, void* v, int vsize) {
	return protocol_mput(p, k, ksize, v, vsize);
}


int
protocol_mget_put_commit(void* p) {
	int rv;
	tcp_handle* h = (tcp_handle*)p;
	int	size = sizeof(int) + evbuffer_get_length(h->mput_buffer);
	int rtype = 8;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	evbuffer_add(msg, &h->mput_buffer_count, sizeof(int));
	evbuffer_add_buffer(msg, h->mput_buffer);
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);
	evbuffer_free(msg);
	if (rv != 0) { return -1; }
	h->mput_buffer_count = 0;
	evbuffer_remove(h->b, &rv, sizeof(int));
	return rv;
}


int 
protocol_btree_search(void* p, long k, long* v) {
	int rv;
    int size = sizeof(long);
	int rtype = 9;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	evbuffer_add(msg, &k, sizeof(long));
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);
	evbuffer_free(msg);
	if (rv != 0) { return -1; }
	evbuffer_remove(h->b, &rv, sizeof(int));
	if (rv >= 0)
		evbuffer_remove(h->b, v, sizeof(long));
	return rv;
}


int
protocol_btree_insert(void* p, long k, long v) {
	int rv;
    int size = 2*sizeof(long);
	int rtype = 10;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	evbuffer_add(msg, &k, sizeof(long));
	evbuffer_add(msg, &v, sizeof(long));
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);
	evbuffer_free(msg);
	if (rv != 0) { return -1; }
	evbuffer_remove(h->b, &rv, sizeof(int));
	return rv;
}


int 
protocol_btree_range(void* p, long min, long max) {
	int i, rv;
    int size = 2*sizeof(long); 
    int rtype = 11;
	long range_count, tmp;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	evbuffer_add(msg, &min, sizeof(long));
	evbuffer_add(msg, &max, sizeof(long));
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);
	evbuffer_free(msg);
	if (rv != 0) { return -1; }
	evbuffer_remove(h->b, &rv, sizeof(int));
	evbuffer_remove(h->b, &range_count, sizeof(long));
	for (i = 0; i < range_count; i++) {
		evbuffer_remove(h->b, &tmp, sizeof(long));
		printf("%ld\n", tmp);
	}
	return 0;
}


int
protocol_btree_update(void* p, long k, long v) {
	int rv;
    int size = 2*sizeof(long);
	int rtype = 12;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	evbuffer_add(msg, &k, sizeof(long));
	evbuffer_add(msg, &v, sizeof(long));
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);
	evbuffer_free(msg);
	if (rv != 0) { return -1; }
	evbuffer_remove(h->b, &rv, sizeof(int));
	return rv;
}

/*static void get_session_result(tcp_handle *h, int *rv, uint16_t *bpt_id)
{
	evbuffer_remove(h->b, rv, sizeof(int));
	evbuffer_remove(h->b, bpt_id, sizeof(uint16_t));
}*/

