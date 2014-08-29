
#include <iostream>
#include <vector>
#include <algorithm>
#include <set>
#include <gtest/gtest.h>
#include "test_helpers.h"

using namespace std;

extern "C" {
	#include "bplustree.h"
	#include <gsl/gsl_rng.h>
	#include <gsl/gsl_randist.h>	
}

class BptreeCoreTest : public testing::Test {
public:
    BptreeCoreTest() {
	   DBUG = false; 
    }

protected:
	
	bptree_session *bps;
	uuid_t nn;
	bool DBUG;
	
	bptree_node * makeRandomMultiBptreeNode(bptree_session *bps, 
		int num_first, int num_second)
	{
		if (num_first * num_second >= BPTREE_NODE_SIZE) return NULL;
		bptree_node *n = bpnode_new();

		//printf("Inserted: ");
		// TODO fill in some data
		int cnt =0;
		for (int i = 0; i < num_first; i++) 
		{
			for (int j =0; j < num_second; j++) 
			{
				int k = i * 10;
				int l = j * 10;
				unsigned char _k[50];
				memcpy(_k, &k, sizeof(k));
				memcpy(_k+sizeof(k), &l, sizeof(l));
				char v[5] = "cccc";
				bptree_key_val kv;
				kv.k = _k;
				kv.v = (unsigned char *)v;
				kv.ksize = 2*sizeof(int);
				kv.vsize = 4;
				//printf(" (%d,%d) -> %s, ",k,l,v);
				copy_key_val_to_node(n,&kv,cnt);
				cnt++;
			}
			
		}
		int rv = is_node_ordered(bps, n);
		assert(rv == 0);
		
		return n;
		
	}
	
	int validateTree() {
		int rv;
		uuid_clear(nn);
		rv = bptree_debug(bps, BPTREE_DEBUG_VERIFY_RECURSIVELY, nn);
		EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
		uuid_clear(nn);
		rv = bptree_debug(bps, BPTREE_DEBUG_VERIFY_SEQUENTIALLY, nn);
		EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	}
	
	bptree_node * makeRandomBptreeNode(bptree_session *bps, int num_elem) {
		if (num_elem > BPTREE_NODE_SIZE) return NULL;
		bptree_node *n = bpnode_new();

		for (int i = 0; i < num_elem; i++) 
		{
			int k = i * 10;
			char v[5] = "cccc";
			bptree_key_val kv;
			kv.k = (unsigned char *)&k;
			kv.v = (unsigned char *)v;
			kv.ksize = sizeof(int);
			kv.vsize = 4;
			copy_key_val_to_node(n,&kv,i);
			
		}
		
		return n;
	}
	
	bptree_session * mockBptreeSessionCreate() {
		
		bptree_session *bps = (bptree_session *) malloc(sizeof(bptree_session));
		memset(bps, 0, sizeof(bps));
		
		bps->bpt_id = 1000;
		bps->tapioca_client_id = 1;
		bps->insert_flags = BPTREE_INSERT_UNIQUE_KEY;
		bps->cursor_node = NULL;
		bps->t = transaction_new();
		return bps;
	}
	
	/*@ Assumes that the k-vs in kv[] have been 'inserted' into node already
	 and were assigned with *v as the value*/
	void verifyKvInNode(bptree_node *x, bptree_key_val *kv, int keys)
	{
		int rv,k_pos, c_pos;
		int positions[20]; 
		// Check exact matches
		for (int i=0; i < keys; i++) {
			rv = find_position_in_node(bps, x, &kv[i], &k_pos, &c_pos);
			positions[i] = k_pos;
			EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
		}
		
		/**************************************************
		* Change value so exact k-v checks should return a position before 
		*/
		// If this is a partial key match, then adjusting the value won't make
		// any difference
		//int shift = 1;
		//if (kv->ksize < x->key_sizes[0]) shift = 0;
		
		sprintf((char *) kv->v,"asdf");
		
		// If we don't set this 'tree' as allowing dupes the value will not be
		// checked
		bps->insert_flags = BPTREE_INSERT_ALLOW_DUPES;
		for (int i=0; i < keys; i++) {
			rv = find_position_in_node(bps, x, &kv[i], &k_pos, &c_pos);
			EXPECT_EQ(rv, BPTREE_OP_KEY_NOT_FOUND);
			EXPECT_EQ(k_pos, positions[i]);
		}
		
		/*********************************************/
		// Confirm that key-only checks still work
		bps->insert_flags = BPTREE_INSERT_UNIQUE_KEY;
		for (int i=0; i < keys; i++) {
			rv = find_position_in_node(bps, x, &kv[i], &k_pos, &c_pos);
			EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
			EXPECT_EQ(k_pos, positions[i]);
		}
		
	}
    
	virtual void SetUp() {
		//system("cd ..; ./start.sh > /dev/null; cd unit");
		bps = mockBptreeSessionCreate();
		//DBUG = false;
	}
	 
	void freeSession() {
		transaction_destroy(bps->t);
		free(bps);
	}
	
	virtual void TearDown() {
		freeSession();
	}

};
