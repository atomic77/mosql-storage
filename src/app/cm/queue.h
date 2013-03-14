#ifndef _QUEUE_H_
#define _QUEUE_H_

#include <stdlib.h>

struct queue;

struct queue* queue_new();
void queue_delete(struct queue* q);
int queue_enq(struct queue* q, void* value, size_t size);
void queue_deq(struct queue* q, void** value, size_t* size);
void queue_deq_timed(struct queue* q, int usec, void** value, size_t* size);

#endif
