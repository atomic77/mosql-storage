#ifndef _VALIDATION_H_
#define _VALIDATION_H_

#include "dsmDB_priv.h"
#include "msg.h"
#include <event2/bufferevent.h>

void init_validation();

int is_validation_buf_full();

int validate_transaction(tr_submit_msg* t);

void reset_validation_buffer();

int validation_cleanup();

int validated_count();

int reorder_counter();

int validation_ST();

int write_conflict_counter();

int write_conflict_prevws_counter();

int too_old_counter();

int validate_phase1(tr_submit_msg* t);
int validate_phase2(tr_submit_msg* t, int commit);

int add_validation_state(struct bufferevent *bev);

#endif /* _VALIDATION_H_ */
