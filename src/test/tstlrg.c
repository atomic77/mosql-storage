#include "tapioca.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

int main(int argc , char **argv){
    tapioca_handle *th;
	int i;
    char* v;
    char* check;
	if (argc < 4) 
	{
		printf("%s <val size> <commit freq> <total keys>\n", argv[0]);
		exit(1);
	}
    int vsize = atoi(argv[1]);
	int commit_freq = atoi(argv[2]);
    int iterations = atoi(argv[3]);
	int commits = 0;
	
    int rv;
    th = tapioca_open("127.0.0.1",5555);

    v = (char*)malloc(vsize);
    check = (char*)malloc(vsize);

    for (i = 0; i < iterations; i++) {
        memset(v, i, vsize);
        rv = tapioca_put(th, &i, sizeof(int), v, vsize);
        if (!rv) printf("put rv %d \n", rv);
		if (i % commit_freq == 0) {
			rv = tapioca_commit(th);
			if (rv >= 0) commits ++;
		}
                                                    }

    for (i = 0; i < iterations; i++) {
        memset(check, i, vsize);
        rv = tapioca_get(th, &i, sizeof(int), v, vsize);
        if (!rv) printf("get rv %d \n ",rv);
        tapioca_commit(th);
		rv = memcmp(check, v, vsize);
        if (rv != 0) printf("memcmp result %d \n", rv); 
    }

    printf("Finished checking %d keys, %d successful commits\n", 
		   iterations, commits);
    free(v);
    free(check);
}
