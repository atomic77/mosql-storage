#include "opt.h"
#include "util.h"
#include "tapioca.h"
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>


static int node_id = -1;


static char* ip = "127.0.0.1";
static int port = 5555;
static int nthreads = 128;
static int nkeys = 1000000;
static int value_size = 4;
static int ops = 2;
static int iterations = 1000000;
static int update_percentage = 100;
static int stored_proc = 0;
static int btree = 0;
static int btree_up = 0;
static char* log_prefix = "";
static int help = 0;

static int spread = 10000000;

static struct option options[] =
{
	{'a', "ip address", &ip, str_opt},
	{'p', "port", &port, int_opt},
	{'t', "number of threads", &nthreads, int_opt},
	{'k', "number of keys", &nkeys, int_opt},
	{'v', "value size", &value_size, int_opt},
	{'o', "operations", &ops, int_opt},
	{'i', "iterations", &iterations, int_opt},
	{'u', "update percentage", &update_percentage, int_opt},
	{'s', "use stored procedures", &stored_proc, fla_opt},
	{'b', "use btree stored procedures (insert)", &btree, fla_opt},
	{'q', "use btree stored procedures (update)", &btree_up, fla_opt},
	{'l', "log prefix", &log_prefix, str_opt},
	{'h', "help", &help, fla_opt},
	{0, 0, 0, 0}
};


static int read_transaction(tapioca_handle* th);
static int read_stored_proc(tapioca_handle* th);
static int update_transaction(tapioca_handle* th);
static int update_stored_proc(tapioca_handle* th);


typedef int (*transaction_fn)(tapioca_handle* th);
transaction_fn do_read = read_transaction;
transaction_fn do_update = update_transaction;


static FILE* log_open(int thread_id) {
	FILE* f;
	char path[128];
	sprintf(path, "%s%d.txt", log_prefix, thread_id);
	f = fopen(path, "w");
	return f;
}


static void log_tx(FILE* f, struct timeval* s, struct timeval* e, int committed, int update) {
    fprintf(f, "%.10ld, %.6u, %.10ld, %.6u, %d, %d\n",
            s->tv_sec,
            (unsigned int)s->tv_usec,
            e->tv_sec, 
            (unsigned int)e->tv_usec,
            committed,
			update);
}



static int update_transaction(tapioca_handle* th) {
	int i, k, n = node_id;
	int min = n * nkeys;
	int max = min + spread;
	char value[value_size];
	for (i = 0; i < (ops/2); i++) {
		k = random_between(min, max);
		tapioca_get(th, &k, sizeof(int), value, value_size);
		tapioca_put(th, &k, sizeof(int), value, value_size);
	}
	return tapioca_commit(th);
}


static int update_stored_proc(tapioca_handle* th) {
	int i, k, n = node_id;
	int min = n * nkeys;
	int max = min + spread;
	char value[value_size];
	for (i = 0; i < (ops/2); i++) {
		k = random_between(min, max);
		tapioca_mget_put(th, &k, sizeof(int), value, value_size);
	}
	return tapioca_mget_put_commit(th);
}


static int read_transaction(tapioca_handle* th) {
	int i, k, n = node_id;
	int min = n * nkeys;
	int max = min + spread;
	char value[value_size];
	for (i = 0; i < ops; i++) {
		k = random_between(min, max);
		tapioca_get(th, &k, sizeof(int), value, value_size);
	}
	return tapioca_commit(th);
}


static int read_stored_proc(tapioca_handle* th) {
	int i, k, n = node_id;
	int min = n * nkeys;
	int max = min + spread;
	for (i = 0; i < ops; i++) {
		k = random_between(min, max);
		tapioca_mget(th, &k, sizeof(int));
	}
	mget_result* res = tapioca_mget_commit(th);
	int rv = (res != NULL) ? 0 : -1;
    mget_result_free(res);
    return rv;
}


static void trace(tapioca_handle* th, FILE* log) {
	int i, r, update, committed;
	int count = iterations / nthreads;
	struct timeval start, end;
	
	for (i = 0; i < count; i++) {
		update = 0;
		r = random_between(0, 100);
		if (r < update_percentage)
			update = 1;
		
		gettimeofday(&start, NULL);
		if (update)
			committed = do_update(th);
		else
			committed = do_read(th);
		gettimeofday(&end, NULL);
		
		log_tx(log, &start, &end, committed, update);
	}
}


static void* trace_thread(void* arg) {
	int id;
	FILE* f;
	tapioca_handle* th;
	
	id = *((int*)arg);
	f = log_open(id);
	if (f == NULL) {
		perror("open");
		return NULL;
	}
	
	th = tapioca_open(ip, port);
	if (th == NULL) {
		printf("Thread %d: tapioca_open() failed\n", id);
		return NULL;
	}
	
	trace(th, f);
	
	tapioca_close(th);
	fclose(f);
	return NULL;
}


static void usage(char const* progname) {
	printf("Usage: %s\n", progname);
	print_options(options);
	exit(1);
}

	
static void print_cmd_line(int argc, char* argv[]) {
	int i;
	for (i = 0; i < argc; i++)
		printf("%s ", argv[i]);
	printf("\n");
}


static void start_threads(int thr, void* (thr_fn)(void*)) {
	int i, rv;
	int id[thr];
	pthread_t threads[thr];
	
	for (i = 0; i < thr; i++) {
		id[i] = i;
		rv = pthread_create(&threads[i], NULL, thr_fn, &(id[i]));
		assert(rv == 0);
	}
	
	for (i = 0; i < thr; i++)
		pthread_join(threads[i], NULL);
}


static int get_node_id() {
	tapioca_handle* th;
	th = tapioca_open(ip, port);
	if (th == NULL) return -1;
	node_id = tapioca_node_id(th);
	tapioca_close(th);
	printf("Connected to node %d\n", node_id);
	return 0;
}


int main(int argc, char *argv[]) {
	int rv;
	
	rv = get_options(options, argc, argv);
	if (rv == -1 || help) usage(argv[0]);
	print_cmd_line(argc, argv);
	
	if (stored_proc) {
		do_read = read_stored_proc;
		do_update = update_stored_proc;
	}
	
	if (btree) {
		// Removed old btree references
	}
	
	if (btree_up) {
		// Removed old btree references
	}
	
	rv = get_node_id();
	if (rv == -1) {
		printf("trace: unable to get node id\n");
		return 1;
	}

	if (spread > nkeys)
	  spread = nkeys;
	
	srandom(node_id);
	start_threads(nthreads, trace_thread);
	
	return 0;
}
