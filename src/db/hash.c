#include "hash.h"

/*
    Jenkins' one at a time hash
*/
unsigned int joat_hash(char* k, int size) {
    unsigned int i, h = 0;
    
    for (i = 0; i < size; i++) {
        h +=  k[i];
        h += (h << 10);
        h ^= (h >> 6);
    }
    h += (h << 3);
    h ^= (h >> 11);
    h += (h << 15);
    
    return h;
}

unsigned int djb2_hash(char *str, int size) {
    unsigned long hash = 5381;
    int i;

    for (i = 0; i < size; i++) {
        hash = ((hash << 5) + hash) + str[i]; /* hash * 33 + c */
    }

    return hash;
}

