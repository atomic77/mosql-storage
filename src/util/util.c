#include "util.h"
#include <stdlib.h>


void timeval_add(struct timeval* t, int usec) {
    int sec;
    sec = usec / 1e6;
    usec -= 1e6 * sec;
    t->tv_sec += sec;
    
    if (t->tv_usec + usec >= 1e6) {
        t->tv_sec++;
        t->tv_usec = (t->tv_usec + usec) - 1e6;
    } else {
        t->tv_usec += usec;
    }
}


int timeval_diff(struct timeval* t1, struct timeval* t2) {
    int us;
    us = (t2->tv_sec - t1->tv_sec) * 1e6;
    if (us < 0) return 0;
    us += (t2->tv_usec - t1->tv_usec);
    return us;
}


int random_between(int min, int max) {
	return (random() % (max - min)) + min;
}


char* random_string(int max_size) {
	char* s;
	int size;
	size = random_between(4, max_size);
	s = malloc(size);
	string_fill_random(s, size);
	return s;
}


void string_fill_random(char* s, int size) {
	int i;
	for (i = 0; i < size; i++)
		s[i] = (char)(random_between((int)'a', (int)'z'));
	s[size-1] = '\0';	
}
