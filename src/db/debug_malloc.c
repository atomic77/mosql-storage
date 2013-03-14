#include <stdlib.h>
#include <stdio.h>

void* db_malloc(size_t size) {
    void * p = malloc(size);
    if (p == NULL) {      
        printf("Malloc failed, out of memory!!!\n");
        exit(1);
    }
    return p;
}
