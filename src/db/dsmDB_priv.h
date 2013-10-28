/*
    Copyright (C) 2013 University of Lugano

	This file is part of the MoSQL storage system. 

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef _DSMDB_PRIV_H_
#define _DSMDB_PRIV_H_

#ifdef __cplusplus
extern "C" {
#endif


#include <stdio.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/time.h>

#include "config.h"

/* 
    Values returned by dsmdb_commit()
*/
#define T_COMMITTED		1
#define T_ABORTED		-1
#define T_ERROR			-2


/*** GLOBAL DEFINES ***/
#define true  1
#define false 0

#define T_HASH_SIZE (sizeof(unsigned int))


#define GET         1
#define PUT         2
#define WRITE       3
#define COMMIT      4
#define CONNECT     5
#define DISCONNECT  6

#define WELCOME     7
#define REFUSED     8

#define BTREE_SEARCH 10
#define BTREE_INSERT 11
#define BTREE_RANGE  12

#define TPC_B        20

#define TEST_UPDATE_SP 30
#define TEST_READ_SP   31

#define REGULAR_NODE 0
#define CACHE_NODE 1

/*** KEY - VALUE ***/
typedef struct db_key_t {
    int   size;
    void* data;
} key;

typedef struct db_val_t {
    int   size;
    int   version;
    void* data;
} val;

key* key_new (void* data, int size);
void key_free (key* k);

val* versioned_val_new (void* data, int size, int version);
val* val_new (void* data, int size);
void val_free (val* v);

/*** STRUCTURES ***/
typedef struct node_info_t {
    char ip[17];
    int port;
    int net_id;
} node_info;


typedef struct tr_id_t {
	int client_id;
	short seqnumber;
	short node_id;
} tr_id;


typedef struct put_message_t {
    int key_size;
    int value_size;
    char data[0];
} put_message;

#define PUT_MSG_SIZE(m) (sizeof(put_message) + m->key_size + m->value_size)

typedef struct get_message_t {
    int key_size;
    char data[0];
} get_message;

#define GET_MSG_SIZE(m) (sizeof(get_message) + m->key_size)


typedef struct flat_key_t {
    int  ksize;
    char data[0];
} flat_key;
#define FLAT_KEY_SIZE(x) (sizeof(flat_key) + x->ksize)


typedef struct flat_key_hash_t {
    unsigned int hash[2];
} flat_key_hash;


typedef struct flat_key_val_t {
    int  ksize;
    int  vsize;
    char data[0];
} flat_key_val;
#define FLAT_KEY_VAL_SIZE(x) (sizeof(flat_key_val) + x->ksize + x->vsize)

// message type recognized by the certifier
#define TRANSACTION_SUBMIT 1 	// tr_submit_msg
#define NODE_JOIN 2				// join_msg
#define RECONFIG 3				// reconf_msg

/*
    data contains:
    - readset hashes (flat_key_hash)
    - writeset hashes (flat_key_hash)
    - writeset data (flat_key_val)
*/
typedef struct tr_submit_msg_t {
	short type;
    tr_id id;
    int   start;
    short readset_count;
    short writeset_count;
    int writeset_size;
    char  data[0];
} tr_submit_msg;

#define TR_SUBMIT_MSG_SIZE(m) ((sizeof(tr_submit_msg)) + ((m->readset_count + m->writeset_count) * sizeof(flat_key_hash)) + (m->writeset_size))
#define TR_SUBMIT_MSG_WS(m) ((char*)&m->data[(m->readset_count + m->writeset_count) * sizeof(flat_key_hash)])
#define TR_SUBMIT_MSG_RS_HASH(m) ((flat_key_hash*)m->data)
#define TR_SUBMIT_MSG_WS_HASH(m) ((flat_key_hash*)&m->data[t->readset_count * sizeof(flat_key_hash)])

//#define TR_MAX_MSG_SIZE 8192
//#define TR_MAX_DATA_SIZE (TR_MAX_MSG_SIZE - sizeof(tr_submit_msg))

typedef struct join_msg_t {
	short type;
	int node_id;
	int port;
	int ST;
	char address[17];
} join_msg;

/*
    data contains:
    - array of aborted tr_ids
    - array of committed tr_ids
    - array of put_msg
*/
typedef struct tr_deliver_msg_t {
	int type;
    int ST;
    int aborted_count;
    int committed_count;
    int updateset_count;
    char data[0];
} tr_deliver_msg;

/* The certifier now sends out a reconfiguration message with the full state of
 the system; data contains:
    array of struct node_info
 */

typedef struct reconf_msg_t {
	short type;
	int ST;
	int nodes;
	char data[0];
} reconf_msg;

//#define DL_MAX_DATA_SIZE 8192


/*** LOGGING MACROS ***/
#define VRB 1
#define V_VRB 2
#define DBG 3
#define V_DBG 4

#define LOG(L, S) if(VERBOSITY_LEVEL >= L) printf S
// Example usage:
//   LOG(VRB, ("verbose: only printed with level greater equal than %d\n", VRB));
//   LOG(V_VRB, ("very_verbose: only printed with level greater equal than %d\n", V_VRB));
//   LOG(DBG, ("debug: only printed with level greater equal than %d\n", DBG));
//   LOG(V_DBG, ("very_debug: only printed with level greater equal than %d\n", V_DBG));

/*** MALLOC DEBUGGING MACROS ***/
#define MALLOC_TRACE_FILENAME "malloc_debug_trace.txt"


void* db_malloc(size_t size);
#define DB_MALLOC(x) db_malloc(x);
#define DB_FREE(x) free(x)


#ifdef __cplusplus
}
#endif

#endif /* _DSMDB_PRIV_H_ */
