#ifndef _MSG_H_
#define _MSG_H_

#include <sys/socket.h>

struct iovmsg {
	struct msghdr header;
	struct iovec* iov;
	int iov_size;
	int iov_used;
};


struct iovmsg* iovmsg_new();
void iovmsg_free(struct iovmsg* m);
void iovmsg_clear(struct iovmsg* m);
void iovmsg_add_iov(struct iovmsg* m, void* buffer, int size);
struct msghdr* iovmsg_header(struct iovmsg* m);
int iovmsg_size(struct iovmsg* m);

#endif
