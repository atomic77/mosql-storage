#include "tapioca.h"
#include "protocol.h"
#include <stdlib.h>


tapioca_handle* 
tapioca_open(const char* address, int port) {
	return protocol_open(address, port);
}


int
tapioca_client_id(tapioca_handle* th) {
	return protocol_client_id(th);
}

int
tapioca_node_id(tapioca_handle* th) {
	return protocol_node_id(th);
}


void 
tapioca_close(tapioca_handle* th) {
	protocol_close(th);
}


int
tapioca_get(tapioca_handle* th, void* k, int ksize, void* v, int vsize) {
	return protocol_get(th, k, ksize, v, vsize);
}


int
tapioca_put(tapioca_handle* th, void* k, int ksize, void* v, int vsize) {
	return protocol_put(th, k, ksize, v, vsize);
}


int
tapioca_commit(tapioca_handle* th) {
	return protocol_commit(th);
}


int
tapioca_rollback(tapioca_handle* th) {
	return protocol_rollback(th);
}


int
tapioca_mget_int(tapioca_handle* th, int n, int* keys, int* values) {
	return protocol_mget_int(th, n, keys, values);
}


int
tapioca_mput_int(tapioca_handle* th, int n, int* keys, int* values) {
	return protocol_mput_int(th, n, keys, values);
}


int 
tapioca_mget(tapioca_handle* th, void* k, int ksize) {
	return protocol_mget(th, k, ksize);
}


mget_result*
tapioca_mget_commit(tapioca_handle* th) {
	return protocol_mget_commit(th);
}


int
tapioca_mput(tapioca_handle* th, void* k, int ksize, void* v, int vsize) {
	return protocol_mput(th, k, ksize, v, vsize);
}


int
tapioca_mput_commit(tapioca_handle* th) {
	return protocol_mput_commit(th);
}


int
tapioca_mput_commit_retry(tapioca_handle* th, int times) {
	return protocol_mput_commit_retry(th, times);
}


int
tapioca_mget_put(tapioca_handle* th, void* k, int ksize, void* v, int vsize) {
	return protocol_mget_put(th, k, ksize, v, vsize);
}


int
tapioca_mget_put_commit(tapioca_handle* th) {
	return protocol_mget_put_commit(th);
}


