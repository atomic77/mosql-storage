#ifndef _PROTOCOL_H_
#define _PROTOCOL_H_

#include "tapioca.h"

//#include <stdint.h>
//#include "../app/tapioca/bplustree_client.h"

void* protocol_open(const char* address, int port);
void protocol_close(void* ph);
int protocol_node_id(void* ph);
int protocol_client_id(void* p);
int protocol_get(void* ph, void* k, int ksize, void* v, int vsize);
int protocol_put(void* ph, void* k, int ksize, void* v, int vsize);
int protocol_commit(void* ph);
int protocol_rollback(void* ph);

int protocol_mget_int(void* ph, int n, int* keys, int* values);
int protocol_mput_int(void* ph, int n, int* keys, int* values);


int protocol_mget(void* ph, void* k, int ksize);
mget_result* protocol_mget_commit(void* ph);

int protocol_mput(void* ph, void* k, int ksize, void* v, int vsize);
int protocol_mput_commit(void* ph);

int protocol_mget_put(void* ph, void* k, int ksize, void* v, int vsize);
int protocol_mget_put_commit(void* ph);
int protocol_mput_commit_retry(void* ph, int times);

#endif
