#include "protocol_btree.h"
#include "tcp_connection.h"
#include "mget_result.h"
#include "tcp_priv.h"
#include <stdlib.h>
#include <string.h>
#include <event2/event.h>
#include <event2/buffer.h>
#include <assert.h>


static void get_bptree_result(tcp_handle *h, int *rv, int32_t *vsize, void *v)
{
	evbuffer_remove(h->b, rv, sizeof(int));
	evbuffer_remove(h->b, vsize, sizeof(int32_t));
	evbuffer_remove(h->b, v, *vsize);
}

// TODO Finish this implementation
static void get_multi_cursor_result(tcp_handle *h, int *rv,
		bptree_mget_result **bmres, int16_t *rows)
{
	bptree_mget_result *cur;
	int i, size;
	int32_t ksize, vsize;
	unsigned char k[32000];
	unsigned char v[32000];

	evbuffer_remove(h->b, rv, sizeof(int));
	evbuffer_remove(h->b, rows, sizeof(int16_t));
	if (*rows > 0 && *rv == BPTREE_OP_KEY_FOUND)
	{
		cur = malloc(sizeof(bptree_mget_result));
		*bmres = cur;
		for (i=1 ; i <= *rows; i++)
		{
			evbuffer_remove(h->b, &cur->ksize, sizeof(int32_t));
			evbuffer_remove(h->b, k, cur->ksize);
			evbuffer_remove(h->b, &cur->vsize, sizeof(int32_t));
			evbuffer_remove(h->b, v, cur->vsize);
			cur->k = malloc(cur->ksize);
			cur->v = malloc(cur->vsize);
			memcpy(cur->k, k, cur->ksize);
			memcpy(cur->v, v, cur->vsize);
			if (i == *rows) break;
			cur->next = malloc(sizeof(bptree_mget_result));
			cur = cur->next;
		}
		cur->next = NULL;
	}
}

static void get_cursor_result(tcp_handle *h, int *rv,
		int32_t *ksize, void *k, int32_t *vsize, void *v)
{
	evbuffer_remove(h->b, rv, sizeof(int));
	evbuffer_remove(h->b, ksize, sizeof(int32_t));
	evbuffer_remove(h->b, k, *ksize);
	evbuffer_remove(h->b, vsize, sizeof(int32_t));
	evbuffer_remove(h->b, v, *vsize);
}

int
protocol_bptree_initialize_bpt_session_no_commit(void *p, uint16_t bpt_id,
		enum bptree_open_flags open_flags, uint32_t execution_id)
{
	int rv;
	int size = sizeof(uint16_t) + sizeof(enum bptree_open_flags)
			+ sizeof(uint32_t);
	int rtype = 14;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	evbuffer_add(msg,&bpt_id, sizeof(uint16_t));
	evbuffer_add(msg,&open_flags, sizeof(enum bptree_open_flags));
	evbuffer_add(msg,&execution_id, sizeof(uint32_t));
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);

	evbuffer_free(msg);
	if (rv != 0) { return -1; }
	evbuffer_remove(h->b, &rv, sizeof(int));
	return rv;
}

int
protocol_bptree_initialize_bpt_session(void *p, uint16_t bpt_id,
		enum bptree_open_flags open_flags)
{
	int rv;
	int size = sizeof(uint16_t) + sizeof(enum bptree_open_flags);
	int rtype = 15;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	evbuffer_add(msg,&bpt_id, sizeof(uint16_t));
	evbuffer_add(msg,&open_flags, sizeof(enum bptree_open_flags));
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);

	evbuffer_free(msg);
	if (rv != 0) { return -1; }
	evbuffer_remove(h->b, &rv, sizeof(int));
	return rv;
}


int protocol_bptree_set_num_fields(void *p,
		tapioca_bptree_id tbpt_id, int16_t num_fields)
{
	int rv;
	int size = sizeof(tapioca_bptree_id) + sizeof(int16_t);
	int rtype = 16;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	evbuffer_add(msg,&tbpt_id, sizeof(tapioca_bptree_id));
	evbuffer_add(msg,&num_fields, sizeof(int16_t));
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);

	evbuffer_free(msg);
	if (rv != 0) { return -1; }
	evbuffer_remove(h->b, &rv, sizeof(int));
	return rv;

}

int
protocol_bptree_set_field_info(void *p,
		tapioca_bptree_id tbpt_id, int16_t field_num,
		int16_t field_sz, enum bptree_field_comparator comparator)
{
	int rv;
	int size = sizeof(tapioca_bptree_id) + sizeof(int16_t)*2 +
			sizeof(enum bptree_field_comparator);
	int rtype = 17;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	evbuffer_add(msg,&tbpt_id, sizeof(tapioca_bptree_id));
	evbuffer_add(msg,&field_num, sizeof(int16_t));
	evbuffer_add(msg,&field_sz, sizeof(int16_t));
	evbuffer_add(msg,&comparator, sizeof(enum bptree_field_comparator));
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);

	evbuffer_free(msg);
	if (rv != 0) { return -1; }
	evbuffer_remove(h->b, &rv, sizeof(int));
	return rv;

}

int protocol_bptree_insert(void *p, tapioca_bptree_id tbpt_id, void *k,
		int ksize, void *v, int vsize, enum bptree_insert_flags insert_flags)
{
	int rv;
	int size = sizeof(tapioca_bptree_id) + sizeof(int32_t)*2
			+ vsize + ksize + sizeof(enum bptree_insert_flags);
	int rtype = 18;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	evbuffer_add(msg,&tbpt_id, sizeof(tapioca_bptree_id));
	evbuffer_add(msg,&ksize, sizeof(int32_t));
	evbuffer_add(msg,k, ksize);
	evbuffer_add(msg,&vsize, sizeof(int32_t));
	evbuffer_add(msg,v, vsize);
	evbuffer_add(msg,&insert_flags, sizeof(enum bptree_insert_flags));
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);

	evbuffer_free(msg);
	if (rv != 0) { return -1; }
	evbuffer_remove(h->b, &rv, sizeof(int));
	return rv;
}

int protocol_bptree_update(void *p, tapioca_bptree_id tbpt_id, void *k,
		int ksize, void *v, int vsize)
{
	int rv;
	int size = sizeof(tapioca_bptree_id)+ sizeof(int32_t)*2 + vsize + ksize;
	int rtype = 19;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	evbuffer_add(msg,&tbpt_id, sizeof(tapioca_bptree_id));
	evbuffer_add(msg,&ksize, sizeof(int32_t));
	evbuffer_add(msg,k, ksize);
	evbuffer_add(msg,&vsize, sizeof(int32_t));
	evbuffer_add(msg,v, vsize);
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);

	evbuffer_free(msg);
	if (rv != 0) { return -1; }
	evbuffer_remove(h->b, &rv, sizeof(int));
	return rv;
}


int protocol_bptree_search(void *p, tapioca_bptree_id tbpt_id, void *k,
		int ksize, void *v, int *vsize)
{
	int rv;
	int size = sizeof(tapioca_bptree_id) + sizeof(int32_t) + ksize;
	int rtype = 20;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	evbuffer_add(msg,&tbpt_id, sizeof(tapioca_bptree_id));
	evbuffer_add(msg,&ksize, sizeof(int32_t));
	evbuffer_add(msg,k, ksize);
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);

	evbuffer_free(msg);
	if (rv != 0) { return -1; }
	get_bptree_result(h, &rv, vsize, v);
	return rv;
}

int protocol_bptree_index_first(void *p, tapioca_bptree_id tbpt_id, void *k,
		int *ksize, void *v, int *vsize)
{
	int rv;
	int size = sizeof(tapioca_bptree_id);
	int rtype = 21;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	evbuffer_add(msg,&tbpt_id, sizeof(tapioca_bptree_id));
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);

	evbuffer_free(msg);
	if (rv < 0) { return -1; }
	get_cursor_result(h, &rv, ksize, k, vsize, v);
	return rv;
}

int protocol_bptree_index_next(void *p, tapioca_bptree_id tbpt_id, void *k,
		int *ksize, void *v, int *vsize)
{
	int rv;
	int size = sizeof(tapioca_bptree_id);
	int rtype = 22;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	evbuffer_add(msg,&tbpt_id, sizeof(tapioca_bptree_id));
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);

	evbuffer_free(msg);
	if (rv < 0) { return -1; }
	get_cursor_result(h, &rv, ksize, k, vsize, v);
	return rv;
}

int protocol_bptree_index_next_mget(void *p, tapioca_bptree_id tbpt_id,
		bptree_mget_result **bmres, int16_t *rows)
{
	int rv;
	int size = sizeof(tapioca_bptree_id);
	int rtype = 23;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	evbuffer_add(msg,&tbpt_id, sizeof(tapioca_bptree_id));
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);

	evbuffer_free(msg);
	if (rv < 0) { return -1; }
	get_multi_cursor_result(h, &rv, bmres, rows);
	return rv;
}

int protocol_bptree_index_first_no_key(void *p, tapioca_bptree_id tbpt_id)
{
	int rv;
	int size = sizeof(tapioca_bptree_id);
	int rtype = 24;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	evbuffer_add(msg,&tbpt_id, sizeof(tapioca_bptree_id));
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);

	evbuffer_free(msg);
	if (rv != 0) { return -1; }
	evbuffer_remove(h->b, &rv, sizeof(int));
	return rv;

}

int protocol_bptree_debug(void *p, tapioca_bptree_id tbpt_id,
		enum bptree_debug_option debug_opt)
{
	int rv;
	int size = sizeof(tapioca_bptree_id) + sizeof(enum bptree_debug_option);
	int rtype = 25;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	evbuffer_add(msg,&tbpt_id, sizeof(tapioca_bptree_id));
	evbuffer_add(msg,&debug_opt, sizeof(enum bptree_debug_option));
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);

	evbuffer_free(msg);
	if (rv != 0) { return -1; }
	evbuffer_remove(h->b, &rv, sizeof(int));
	return rv;

}

int protocol_bptree_delete(void *p, tapioca_bptree_id tbpt_id, void *k,
		int ksize, void *v, int vsize)
{
	int rv;
	int size = sizeof(tapioca_bptree_id)+ sizeof(int32_t)*2 + vsize + ksize;
	int rtype = 26;
	tcp_handle* h = (tcp_handle*)p;
	struct evbuffer* msg = buffer_with_header(size, rtype);
	evbuffer_add(msg,&tbpt_id, sizeof(tapioca_bptree_id));
	evbuffer_add(msg,&ksize, sizeof(int32_t));
	evbuffer_add(msg,k, ksize);
	evbuffer_add(msg,&vsize, sizeof(int32_t));
	evbuffer_add(msg,v, vsize);
	CHECK_BUFFER_SIZE(msg, size);
	rv = tcp_write_buffer(h->c, msg, h->b);

	evbuffer_free(msg);
	if (rv != 0) { return -1; }
	evbuffer_remove(h->b, &rv, sizeof(int));
	return rv;
}
