#include "tapioca.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <limits.h>

#include "dsmDB_priv.h"
#include <libpaxos/storage.h>
#include <paxos.h>
#include <libpaxos/libpaxos_messages.h>
#include <evpaxos/config.h>
#include "config_reader.h"
#include "socket_util.h"
#include "../app/rec/index.h"
#include "recovery_learner_msg.h"
#include "peer.h"
#include "hash.h"
#include "tapiocadb.h"
#include "carray.h"

static void add_keys_to_hash(tr_deliver_msg* dmsg) {
	int i, offset;
	flat_key_val* kv;
	key k;
	val v;
	tr_id *t;

	// It would seem that we can't really verify anything unless we have
	// the readset as well..
	offset = (dmsg->aborted_count + dmsg->committed_count) * sizeof(tr_id);

	for (i = 0; i < dmsg->updateset_count; i++) {
    	kv = (flat_key_val*)&dmsg->data[offset];
	    k.size = kv->ksize;
	    k.data = kv->data;
	    v.size = kv->vsize;
	    v.data = &kv->data[k.size];
	    v.version = dmsg->ST;
    	offset += FLAT_KEY_VAL_SIZE(kv);
	}
}

/*@ Scan through the recovery index to find the lowest iid;
 *  anything below this can be purged from the acceptors as it
 * implies that we have a new version of every key contained therein
 */
static iid_t find_min_rec_iid(int acceptor_id) {
	iid_t iid, min_iid;
	int rv;
	char rec_db_path[128];
	//char key[4096];
	
	min_iid = INT_MAX;
	sprintf(rec_db_path, "%s/rlog_%d", "/tmp", acceptor_id);
	rlog *rl = rlog_init(rec_db_path);
	key k;
	
	rlog_tx_begin(rl);
	while ( (rv = rlog_next(rl, &iid, &k)) == 1 ) 
	{
		if (iid < min_iid) min_iid = iid;
		
	}
	rlog_tx_commit(rl);
	rlog_close(rl);
	
	
	return min_iid;
	
}

int main(int argc, char const *argv[]) {
	
	iid_t iid, start_iid, max_iid, min_rec_iid;
	accept_ack *ar;
	tr_deliver_msg *tr;
	int acceptor_id;
	
	if (argc != 3) 
	{
		printf("Usage: %s <acceptor id> <start iid>\n", argv[0]);
		return 1;
	}
	
	acceptor_id = atoi(argv[1]);
	start_iid = atoi(argv[2]);
	
	// Open acceptor logs
    struct storage * ssm = storage_open(0);
    assert(ssm != NULL);

	storage_tx_begin(ssm);
	max_iid = storage_get_max_iid(ssm);
	storage_tx_commit(ssm);
	
	printf("Maximum iid found: %ld\n", max_iid);
	
	
	for(iid = start_iid; iid <= max_iid; iid++) 
	{
		
		storage_tx_begin(ssm);
		ar = storage_get_record(ssm,iid);
		storage_tx_commit(ssm);
		assert(ar != NULL);
		
		if (ar->value_size == 0) continue;
		tr = (tr_deliver_msg *)ar->value;
		printf("iid: %d ST: %d abrt, comm, updset count: %d, %d, %d\n", 
			   iid, tr->ST, tr->aborted_count, tr->committed_count, 
				tr->updateset_count);
	
	
	}
	storage_close(ssm);
	
	min_rec_iid = find_min_rec_iid(0);
	printf("Min rec iid %d \n", min_rec_iid);
	
	
}
