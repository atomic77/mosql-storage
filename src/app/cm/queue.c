#include "queue.h"
#include <errno.h>
#include <pthread.h>
#include <sys/time.h>


struct node {
	void* value;
	size_t size;
	struct node* next;
};


struct queue {
	int size;
	struct node* tail;
	pthread_mutex_t enq_m;
	struct node* head;
	pthread_mutex_t deq_m;
	pthread_cond_t not_empty_cond;
};


static int atomic_inc(int* x) {
	return __sync_fetch_and_add(x, 1);
}


static int atomic_dec(int* x) {
	return __sync_fetch_and_add(x, -1);
}


static int atomic_get(int* x) {
	return __sync_fetch_and_add(x, 0);
}


struct node* node_new(void* value, size_t size) {
	struct node* n;
	n = (struct node*)malloc(sizeof(struct node));
	n->value = value;
	n->size = size;
	n->next = NULL;
	return n;
}


struct queue* queue_new() {
	struct queue* q;
	q = (struct queue*)malloc(sizeof(struct queue));
	if (q == NULL) return NULL;
	q->size = 0;
	q->head = node_new(NULL, -1);
	q->tail = q->head;
	pthread_mutex_init(&q->enq_m, NULL);
	pthread_mutex_init(&q->deq_m, NULL);
	pthread_cond_init(&q->not_empty_cond, NULL);
	return q;
}


void queue_delete(struct queue* q) {
	struct node* n;
	while (q->head) {
		n = q->head;
		q->head = q->head->next;
		if (n != q->tail)
			free(n->value);
		free(n);
	}
	pthread_mutex_destroy(&q->enq_m);
	pthread_mutex_destroy(&q->deq_m);
	pthread_cond_destroy(&q->not_empty_cond);
	free(q);
}


int queue_enq(struct queue* q, void* value, size_t size) {
	struct node* n = node_new(value, size);
	
	pthread_mutex_lock(&q->enq_m);
	q->tail->next = n;
	q->tail = n;
	pthread_mutex_unlock(&q->enq_m);
	
	if (atomic_inc(&q->size) == 0) {
		pthread_mutex_lock(&q->deq_m);
		pthread_cond_signal(&q->not_empty_cond);
		pthread_mutex_unlock(&q->deq_m);
	}
	
	return 0;
}


void queue_deq(struct queue* q, void** value, size_t* size) {
	struct node* n;
	
	pthread_mutex_lock(&q->deq_m);
	while (atomic_get(&q->size) == 0) {
		pthread_cond_wait(&q->not_empty_cond, &q->deq_m);
	}
	n = q->head;
	*size  = n->next->size;
	*value = n->next->value;
	q->head = q->head->next;
	pthread_mutex_unlock(&q->deq_m);
	
	atomic_dec(&q->size);
	free(n);
}


static void set_timeout(struct timespec* ts, int usec) {
    struct timeval tv;
	gettimeofday(&tv, NULL);
    ts->tv_sec = tv.tv_sec;
    int us = usec + tv.tv_usec;
    if (us >= 1000*1000) {
        ts->tv_nsec = (us % 1000*1000) * 1000;
        ts->tv_sec += 1;
    } else {
        ts->tv_nsec = us * 1000;
    }
}


void queue_deq_timed(struct queue* q, int usec, void** value, size_t* size) {
	int rv;
	struct node* n;
	struct timespec ts;
	set_timeout(&ts, usec);
	
	pthread_mutex_lock(&q->deq_m);
	while (atomic_get(&q->size) == 0) {
		rv = pthread_cond_timedwait(&q->not_empty_cond, &q->deq_m, &ts);
		if (rv == ETIMEDOUT) {
			pthread_mutex_unlock(&q->deq_m);
			*size = 0;
			*value = NULL;
			return;
		}
	}
	n = q->head;
	*size  = n->next->size;
	*value = n->next->value;
	q->head = q->head->next;
	pthread_mutex_unlock(&q->deq_m);

	atomic_dec(&q->size);
	free(n);
}
