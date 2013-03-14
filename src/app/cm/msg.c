#include "msg.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>


#define IOV_SIZE_MAX 512


struct iovmsg* iovmsg_new() {
	struct iovmsg* m;
	m = malloc(sizeof(struct iovmsg));
	memset(&m->header, 0, sizeof(struct msghdr));
	m->iov = malloc(IOV_SIZE_MAX * sizeof(struct iovec));
	iovmsg_clear(m);
	return m;
}


void iovmsg_free(struct iovmsg* m) {
	free(m);
}


void iovmsg_clear(struct iovmsg* m) {
	m->iov_used = 0;
	m->iov_size = 0;
}


void iovmsg_add_iov(struct iovmsg* m, void* buffer, int size) {
	struct iovec* iv = &m->iov[m->iov_used];
	iv->iov_base = buffer;
	iv->iov_len  = size;
	m->iov_used++;
	m->iov_size += size;
	assert(m->iov_used < IOV_SIZE_MAX);
}


struct msghdr* iovmsg_header(struct iovmsg* m) {
	m->header.msg_iov = m->iov;
	m->header.msg_iovlen = m->iov_used;
	return &m->header;
}


int iovmsg_size(struct iovmsg* m) {
	return m->iov_size;
}
