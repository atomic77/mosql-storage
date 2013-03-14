#ifndef _TCP_H_
#define _TCP_H_

#include "transaction.h"

//#define TRACE_MODE

//FILE *trace_fp;
//int file_opened = 0;

int tcp_init(int port);
int tcp_cleanup(void);
void on_commit(tr_id* id, int commit_result);

struct hashtable* clients;

#endif
