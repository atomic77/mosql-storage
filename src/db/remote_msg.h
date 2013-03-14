#ifndef _REMOTE_MSG_
#define _REMOTE_MSG_
	

#define REMOTE_GET 1
#define REMOTE_PUT 2


// Remote messages

typedef struct remote_message_t {
	int type;
	char data[0];
} remote_message;


typedef struct remote_get_message_t {
	int st;
	int req_id;
    int sender_node;
    int version;
    int key_size;
    char data[0];
} remote_get_message;

#define REMOTE_GET_MSG_SIZE(m) (sizeof(remote_get_message) + m->key_size)


typedef struct remote_put_message_t {
	int req_id;
    int key_size;
    int value_size;
    int val_version;
    int req_version;
	int cache;
    char data[0];
} remote_put_message;

#define REMOTE_PUT_MSG_SIZE(m) (sizeof(remote_put_message) + m->key_size + m->value_size)


// Recovery messages

typedef struct recovery_message_t {
    int node_id;
} recovery_message;


typedef struct rec_map_batch_t {
    int count;
    char data[0];
} rec_map_batch;


typedef struct rec_key_batch_t {
    int done;
    int count;
    char data[0];
} rec_key_batch;

#endif
