#include "opt.h"
#include "tapioca.h"
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>
#include <string.h>


static char* ip = "127.0.0.1";
static int port = 5555;
static int nthreads = 100;
static int vsize = 4;
static int nkeys = 1000000;
static int cache = 1;
static int btree = 0;
static int spread = 1000000;
static int isget = 0;
static int help = 0;


static struct option options[] =
{
	{'a', "ip address", &ip, str_opt},
	{'p', "port", &port, int_opt},
	{'t', "number of threads", &nthreads, int_opt},
	{'k', "number of keys", &nkeys, int_opt},
	{'v', "value size", &vsize, int_opt},
	{'c', "do not cache populated data", &cache, fla_opt},
	{'s', "spread", &spread, int_opt},
	{'b', "populate btree", &btree, fla_opt},
	{'g', "verify data", &isget, fla_opt},
	{'h', "help", &help, fla_opt},
	{0, 0, 0, 0}
};


static void* verify_thread(void* arg) {
	int rv, i, id, node_id;
	int from, to;
	tapioca_handle* th;
	char value[vsize+1];
	char vbuf[vsize+1];
	char kstr[vsize+1];
	char snum[128];
	int commit_every;
	if (vsize > 4096)
		commit_every = 1;
	else
	 	commit_every = (4096/vsize > 8) ? 8 : (4096 / vsize);

	commit_every = 1;
	id = *((int*)arg);
	th = tapioca_open(ip, port);
	if (th == NULL) {
		printf("Thread %d: tapioca_open() failed\n", id);
		return NULL;
	}

	node_id = tapioca_node_id(th);

	from = (nkeys * node_id) + (id * (nkeys/nthreads));
	to   = from + (nkeys/nthreads);

	memset(value, '\101', vsize);
	vbuf[vsize] = value[vsize] = '\0';

	for (i = from; i < to; i++) {
		rv = sprintf(value, "%d", i); // embed the key # into the value
		memset(value+rv, '\101', vsize-rv);
		tapioca_get(th, &i, sizeof(int), vbuf, vsize);
		if (memcmp(value, vbuf, vsize) != 0) {
			printf("Mismatch between %s and %s\n", value, vbuf);
			break;
		}
		if (i % commit_every == 0) {
			rv = tapioca_commit(th);
			if (rv == -1)
				i -= commit_every;
		}
	}
	tapioca_commit(th);

	printf("verif. thread %d: %d - %d\n", id, from, to);
	tapioca_close(th);

	return NULL;
}


static void* populate_thread(void* arg) {
	int rv, i, id, node_id;
	int from, to;
	tapioca_handle* th;
	char value[vsize+1];
	char kstr[vsize];
	int commit_every;
	if (vsize > 4096)
		commit_every = 1;
	else
	 	commit_every = (4096/vsize > 8) ? 8 : (4096 / vsize);
	
	commit_every = 1;
	id = *((int*)arg);
	th = tapioca_open(ip, port);
	if (th == NULL) {
		printf("Thread %d: tapioca_open() failed\n", id);
		return NULL;
	}
	
	node_id = tapioca_node_id(th);
	
	from = (nkeys * node_id) + (id * (nkeys/nthreads));
	to   = from + (nkeys/nthreads);
	
	memset(value, '\101', vsize);

	for (i = from; i < to; i++) {
		rv = sprintf(value, "%d", i); // embed the key # into the value
		memset(value+rv, '\101', vsize-rv);
		tapioca_put(th, &i, sizeof(int), value, vsize);
		if (i % commit_every == 0) {
			rv = tapioca_commit(th);
			if (rv == -1) 
				i -= commit_every;
		}
	}
	tapioca_commit(th);
	
	if (cache) {
		from = (nkeys * node_id) + (id * (spread/nthreads));
		to = from + (spread/nthreads);
	
		for (i = from; i < to; i++) {
			  tapioca_get(th, &i, sizeof(int), value, vsize);
			  if (i % commit_every == 0) tapioca_commit(th);
			}
		tapioca_commit(th);
	}
		
	printf("thread %d: %d - %d\n", id, from, to);
	tapioca_close(th);
	
	return NULL;
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
	
	for (i = 0; i < thr; i++) {
		pthread_join(threads[i], NULL);
	}
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


int main(int argc, char *argv[]) {
	int rv;
	
	rv = get_options(options, argc, argv);
	if (rv == -1 || help) usage(argv[0]);
	print_cmd_line(argc, argv);
	
	if (spread > nkeys)
		spread = nkeys;
	
	if (btree) { }
//		start_threads(nthreads, populate_btree_thread);
	else if (isget)
		start_threads(nthreads, verify_thread);
	else
		start_threads(nthreads, populate_thread);
	return 0;
}
