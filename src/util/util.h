#ifndef _UTIL_H_
#define _UTIL_H_

#include <sys/time.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
	Increments t by usec microseconds.
*/	
void timeval_add(struct timeval* t, int usec);


/**
	Returns t2 - t1 in microseconds.
*/
int timeval_diff(struct timeval* t1, struct timeval* t2);


/**
    Returns a random number [min, max)
*/
int random_between(int min, int max);


/**
	Returns a NULL-terminated randomly generated string (both its size and
	contents are random). The returned string is at most max_size bytes long.
*/
char* random_string(int max_size);


/*
	Fill a the given string s with size random characters. The string is 
	NULL-terminated.
*/
void string_fill_random(char* s, int size);

#ifdef __cplusplus
}
#endif

#endif
