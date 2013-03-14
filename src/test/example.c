#include "opt.h"
#include "tapioca.h"
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>


static char* address = "127.0.0.1";
static int port = 8080;
static int iterations = 10;
static int help = 0;


static struct option options[] =
{
	{'a', "address", &address, str_opt},
	{'p', "port", &port, int_opt},
	{'i', "number of iterations", &iterations, int_opt},
	{'h', "help", &help, fla_opt},
	{0, 0, 0, 0}
};


static int tapioca_put_int(tapioca_handle* th, int k, int v)  {
	int rv;
 	rv = tapioca_put(th, &k, sizeof(int), &v, sizeof(int));
	if (rv == -1) return -1;
	return tapioca_commit(th);
}


static int tapioca_get_int(tapioca_handle* th, int k, int* v) {
	int rv;
	rv = tapioca_get(th, &k, sizeof(int), v, sizeof(int));
	if (rv == -1) return -1;
	return tapioca_commit(th);
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
	int i, v, rv;
	tapioca_handle* th;
	
	rv = get_options(options, argc, argv);
	if (rv == -1 || help) usage(argv[0]);
	print_cmd_line(argc, argv);
	
	printf("%s%d\n", address, port);
	th = tapioca_open(address, port);
	assert(th != NULL);
	
	for (i = 0; i < iterations; i++) {
		rv = tapioca_put_int(th, i, i);
		if (rv != -1)
			printf("put k: %d v: %d, OK\n", i, i);
		else
			printf("put k: %d v: %d, failed\n", i, i);
	}
	
	for (i = 0; i < iterations; i++) {
		rv = tapioca_get_int(th, i, &v);
		assert(rv != -1);
		if (rv == 0) {
			printf("get k: %d v: no value\n", i);
		} else {
			printf("get k: %d v: %d\n", i, v); 
		}
	}
	
	tapioca_close(th);
	
	return 0;
}
