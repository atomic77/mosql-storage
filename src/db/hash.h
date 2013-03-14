#ifndef _HASH_H_
#define _HASH_H_

typedef unsigned int (*hash_fun)(char*, int);

unsigned int joat_hash (char* k, int size);

unsigned int djb2_hash (char *str, int size);

#endif /* _HASH_H_ */

