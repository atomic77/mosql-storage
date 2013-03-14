#include "mget_result.h"
#include <stdlib.h>
#include <event2/event.h>
#include <event2/buffer.h>


int mget_result_consume(mget_result* res, void* v) {
    int bytes;
    if (res->count <= 0) 
        return -1;
    evbuffer_remove(res->buffer, &bytes, sizeof(int));
	evbuffer_remove(res->buffer, v, bytes);
    res->count--;
    return bytes;
}


int mget_result_count(mget_result* res) {
    return res->count;
}


void mget_result_free(mget_result* res) {
    evbuffer_free(res->buffer);
    free(res);
}
