#include "bloom.h"
#include "validation.h"

#include <stdlib.h>
#include <memory.h>


#define MAX_ABORT_COUNT 512
#define SKIP_VALIDATION 0

typedef struct {
	int type;
    int ST;
    int abort_count;
    int commit_count;
	int update_set_count;
    bloom* ws;
    bloom** snapshots;
    tr_id* abort_tr_ids;
    tr_id* commit_tr_ids;
} validation_state;


typedef struct {
    int offset;
    char* data;
} buffer;


static buffer* us_buffer;
static validation_state vs;


static int too_old = 0;
static int ws_conflict = 0;
static int prevws_conflict = 0;


static void buffer_clear(buffer* b) {
    b->offset = 0;
}


static void buffer_add(buffer* b, char* data, int size) {
    memcpy(&b->data[b->offset], data, size);
    b->offset += size;
}


void init_validation() {
    int i;
	vs.type = TRANSACTION_SUBMIT;
    vs.ST = 0;
    vs.abort_count = 0;
    vs.commit_count = 0;
	vs.update_set_count = 0;
    vs.ws = bloom_new(BIG_BLOOM);
    vs.snapshots = DB_MALLOC(sizeof(bloom*) * MaxPreviousST);
    for (i = 0; i < MaxPreviousST; i++)
        vs.snapshots[i] = bloom_new(BIG_BLOOM);
    vs.abort_tr_ids = DB_MALLOC(sizeof(tr_id) * MAX_ABORT_COUNT);
    vs.commit_tr_ids = DB_MALLOC(sizeof(tr_id) * ValidationBufferSize);
    us_buffer = DB_MALLOC(sizeof(buffer));
    us_buffer->data = DB_MALLOC(64*1024);
    buffer_clear(us_buffer);
}


int validation_ST() {
    return vs.ST;
}


int validated_count() {
    return vs.abort_count + vs.commit_count;
}


int reorder_counter() {
    return 0;
}


int write_conflict_counter() {
    return ws_conflict;
}


int write_conflict_prevws_counter() {
    return prevws_conflict;
}


int validation_cleanup() {
    return 1;
}


int too_old_counter() {
    return too_old;
}


void reset_validation_buffer() {
    int i;
    bloom* tmp;
    
    vs.abort_count = 0;
    vs.commit_count = 0;
	vs.update_set_count = 0;
    
    tmp = vs.ws; 
    vs.ws = vs.snapshots[MaxPreviousST-1];
    bloom_clear(vs.ws);
    for (i = MaxPreviousST-1; i > 0; i--)
        vs.snapshots[i] = vs.snapshots[i-1];
    vs.snapshots[0] = tmp;

	buffer_clear(us_buffer);
}


int is_validation_buf_full() {
    return (vs.commit_count >= ValidationBufferSize) ||
           (vs.abort_count >= MAX_ABORT_COUNT);
}


static void abort_transaction(tr_submit_msg* t) {
    vs.abort_tr_ids[vs.abort_count] = t->id;
    vs.abort_count++;
}


static void commit_transaction(tr_submit_msg* t) {
    int i;
    flat_key_hash* ws_hashes;
    
	if (!SKIP_VALIDATION) {
    	ws_hashes = TR_SUBMIT_MSG_WS_HASH(t);
    	for (i = 0; i < t->writeset_count; i++)
        	bloom_add_hashes(vs.ws, ws_hashes[i].hash);
    }
	
    vs.commit_tr_ids[vs.commit_count] = t->id;
    vs.commit_count++;
    
    char* ws = TR_SUBMIT_MSG_WS(t);
    buffer_add(us_buffer, ws, t->writeset_size);
	vs.update_set_count += t->writeset_count;
}


static int validate_snapshots(tr_submit_msg* t, flat_key_hash* rs_hashes) {
    int i, j;
    for (i = 0; i < (vs.ST - t->start); i++)
        for (j = 0; j < t->readset_count; j++)
            if (bloom_contains_hashes(vs.snapshots[i], rs_hashes[j].hash))
                return 0;
    return 1;
}


static int validate(tr_submit_msg* t) {
    int i;
    flat_key_hash* rs_hashes;
    
    rs_hashes = TR_SUBMIT_MSG_RS_HASH(t);
    
    // Check readset of t against writesets of old snapshots
    if (vs.ST > t->start) {
        if (validate_snapshots(t, rs_hashes) == 0) {
            prevws_conflict++;
            return 0;
        }
    }
	
    // Check readset of t against writeset of current snapshot
    for (i = 0; i < t->readset_count; i++) {
        if (bloom_contains_hashes(vs.ws, rs_hashes[i].hash)) {
            ws_conflict++;
            return 0;
        }
    }
    
    return 1;
}


int validate_transaction(tr_submit_msg* t) {
	if (SKIP_VALIDATION) {
		commit_transaction(t);
		return 1;
	}
	
    if ((t->start >= (vs.ST - MaxPreviousST)) && (t->start <= vs.ST)) {
        if (validate(t)) {
            commit_transaction(t);
            return 1;
        }
    } else {
        too_old++;
    }
    abort_transaction(t);
    return 0;
}


int validate_phase1(tr_submit_msg* t) {
	flat_key_hash* rs_hashes;
	
	if (t->start >= (vs.ST - MaxPreviousST)) {
		rs_hashes = TR_SUBMIT_MSG_RS_HASH(t);
		if (validate_snapshots(t, rs_hashes) == 0) {
			prevws_conflict++;
			return 0;
		}
    } else {
        too_old++;
		return 0;
    }
    return 1;
}


int validate_phase2(tr_submit_msg* t, int commit) {
	int i;
	flat_key_hash* rs_hashes;
	
	if (commit) {
		rs_hashes = TR_SUBMIT_MSG_RS_HASH(t);
		for (i = 0; i < t->readset_count; i++) {
	        if (bloom_contains_hashes(vs.ws, rs_hashes[i].hash)) {
	            ws_conflict++;
				commit = 0;
				break;
	        }
	    }
	}
	
	if (commit)
		commit_transaction(t);
	else 
		abort_transaction(t);

	return commit;
}


// Write out validation_state/tr_deliver_msg; returns the number of bytes written
int add_validation_state(struct bufferevent *bev) {
	int size, written;
	
	written = 0;
	// we don't expect more transactions for this ST
    vs.ST++;
	
    // Top 5 elements of validation state are in common with tr_deliver_msg
	size = 5 * sizeof(int);
	bufferevent_write(bev, &vs, size);
	written += size;
	
	// add abort tr_ids
	size = vs.abort_count * sizeof(tr_id);
	bufferevent_write(bev, vs.abort_tr_ids, size);
	written += size;
	
	// add commit tr_ids
	size = vs.commit_count * sizeof(tr_id);
	bufferevent_write(bev, vs.commit_tr_ids, size);
	written += size;
	
	// add update set buffer
	bufferevent_write(bev, us_buffer->data, us_buffer->offset);
	written += us_buffer->offset;
	return written;
}
