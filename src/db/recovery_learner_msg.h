#ifndef _RECOVERY_LEARNER_MSG_H_
#define _RECOVERY_LEARNER_MSG_H_

#define REC_KEY_MSG 	0
#define REC_KEY_REPLY	101

/*
	TODO needs ST???
*/
typedef struct rec_key_msg_t {
	int type;
	int req_id;
	int node_id;
	int ksize;
	char data[0];
} rec_key_msg;


typedef struct rec_key_reply_t {
	int type;
	int req_id;
	int size;
	int version;
	char data[0];
} rec_key_reply;

#endif
