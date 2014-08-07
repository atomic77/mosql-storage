/*
    Copyright (C) 2013 University of Lugano

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

/**

 * Adapted from integer-based internal btree from tapioca sources
 *
 * This is a b+tree implementation that operates entirely external to tapioca
 * cluster and uses the basic get/put/commit API to provide bptree functionality
 * Arbitrary binary data can be indexed provided that a pointer to a function
 * is provided for ordering (similar to qsort())
 */
#include "gtest.h"
extern "C" {
#include "bplustree.h"
	#include <assert.h>
}

class BptreeCoreTest : public testing::Test {

protected:
	
	bptree_session *bps;
	uuid_t nn;
	bool DBUG;
	
	bptree_node * makeRandomMultiBptreeNode(bptree_session *bps, 
		int num_first, int num_second)
	{
		if (num_first * num_second >= BPTREE_NODE_SIZE) return NULL;
		bptree_node *n = create_new_bptree_node(bps);

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
		//printf("\n");
		assert(is_node_ordered(bps, n) == 0);
		
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
		bptree_node *n = create_new_bptree_node(bps);

			// TODO fill in some data

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
		int rv,pos;
		int positions[20]; 
		// Check exact matches
		for (int i=0; i < keys; i++) {
			positions[i] = find_position_in_node(bps, x, &kv[i], &rv);
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
			pos = find_position_in_node(bps, x, &kv[i], &rv);
			EXPECT_EQ(rv, BPTREE_OP_KEY_NOT_FOUND);
			EXPECT_EQ(pos, positions[i]);
		}
		
		/*********************************************/
		// Confirm that key-only checks still work
		bps->insert_flags = BPTREE_INSERT_UNIQUE_KEY;
		for (int i=0; i < keys; i++) {
			pos = find_position_in_node(bps, x, &kv[i], &rv);
			EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
			EXPECT_EQ(pos, positions[i]);
		}
		
	}
    
	virtual void SetUp() {
		//system("cd ..; ./start.sh > /dev/null; cd unit");
		bps = mockBptreeSessionCreate();
		DBUG = false;
	}
	
	virtual void TearDown() {
	}

};

class BptreeIntBasedTreeTest : public BptreeCoreTest {
	
protected:
	int rv;
	int k, v, ksize, vsize;
	virtual void SetUp() {
		//system("cd ..; ./start.sh > /dev/null; cd unit");
		bps = mockBptreeSessionCreate();
		createNewMockSession();
	}
	
	bptree_node * makeIncrementalBptreeNode(bptree_session *bps, 
						int start, int end,
						int num_elem) 
	{
		// Make a roughly spaced node with n elements
		if (num_elem > BPTREE_NODE_SIZE) return NULL;
		bptree_node *n = create_new_bptree_node(bps);
		
		int incr = (end - start) / (num_elem);

		for (int i = 0; i < num_elem; i++) 
		{
			int k = start + (i * incr);
			int v = k*100;
			bptree_key_val kv;
			kv.k = (unsigned char *)&k;
			kv.v = (unsigned char *)&v;
			kv.ksize = sizeof(k);
			kv.vsize = sizeof(v);
			copy_key_val_to_node(n,&kv,i);
			
		}
		assert(is_node_ordered(bps, n) == 0);
		
		return n;
		
	}
	
	void createMiniTree(bptree_node **parent, 
				   bptree_node **cleft, int l_sz,
				   bptree_node **cright, int r_sz,
				   bool hasChildren) 
	{
		
		bptree_node *p, *cl, *cr;
		p = create_new_bptree_node(bps);
		p->leaf = 0;
		cl = makeIncrementalBptreeNode(bps, 100, 200, l_sz);
		cr = makeIncrementalBptreeNode(bps, 300, 400, r_sz);
		bptree_key_val *kv = copy_key_val_from_node(cr, 0);
		if (hasChildren) {
			int a = 250;
			memcpy(kv->k, &a, sizeof(int));
		}
		copy_key_val_to_node(p, kv, 0);
		uuid_copy(p->children[0],cl->self_key);
		uuid_copy(p->children[1],cr->self_key);
		
		*parent = p;
		*cleft = cl;
		*cright = cr;
		
		if (hasChildren) {
			// Create fake children
			cl->leaf = 0;
			cr->leaf = 0;
			for (int i =0; i <= cl->key_count; i++) {
				uuid_generate_random(cl->children[i]);
			}
			for (int i =0; i <= cr->key_count; i++) {
				uuid_generate_random(cr->children[i]);
			}
		} else {
			
			uuid_copy(cl->next_node, cr->self_key);
		}
		
		EXPECT_EQ(is_valid_traversal(bps, p, cl, 0), 0);
		EXPECT_EQ(is_valid_traversal(bps, p, cr, 1), 0);
		
		EXPECT_EQ(is_node_sane(p), 0);
		EXPECT_EQ(is_node_sane(cl), 0);
		EXPECT_EQ(is_node_sane(cr), 0);
		
		
	}
	
	void miniTreeSanityChecks(bptree_node *p, bptree_node *cl, 
					 bptree_node *cr)
	{
		
		EXPECT_EQ(is_node_sane(p), 0);
		EXPECT_EQ(is_node_sane(cl), 0);
		EXPECT_EQ(is_node_sane(cr), 0);
		EXPECT_EQ(is_node_ordered(bps,cl), 0);
		EXPECT_EQ(is_node_ordered(bps,cr), 0);
		
		EXPECT_EQ(is_valid_traversal(bps,p,cl,0), 0);
		EXPECT_EQ(is_valid_traversal(bps,p,cr,1), 0);
		
		EXPECT_EQ(uuid_compare(p->children[0], cl->self_key), 0);
		EXPECT_EQ(uuid_compare(p->children[1], cr->self_key), 0);
	
		if (!cl->leaf) {
			int c;
			/* Sanity checks will find simple corruptions, but 
			 * make sure we didn't do any funky copying or duplication*/
			for (int l =0; l < cl->key_count; l++) {
				c = uuid_compare(cl->children[l], 
						 cl->children[l+1]);
				EXPECT_NE(c, 0);
			}
			
			for (int r =0; r < cr->key_count; r++) {
				c = uuid_compare(cl->children[r], 
						 cl->children[r+1]);
				EXPECT_NE(c, 0);
			}
		} else  {
			EXPECT_EQ(uuid_compare(cl->next_node, cr->self_key), 0);
		}
	}
	
	
	
	void createNewMockSession() {
		createNewMockSession(1000, 
				     BPTREE_OPEN_OVERWRITE, 
				     BPTREE_INSERT_UNIQUE_KEY);
	}
	void createNewMockSession(tapioca_bptree_id bpt_id, 
				  bptree_open_flags o_f, bptree_insert_flags i_f)
	{
		bptree_initialize_bpt_session(bps, bpt_id, o_f, i_f);
		bptree_set_num_fields(bps, 1);
		bptree_set_field_info(bps, 0, sizeof(int32_t), 
				      BPTREE_FIELD_COMP_INT_32, int32cmp);
		
	}
	
	
};

TEST_F(BptreeCoreTest, NodeSerDe) {
	size_t bsize1, bsize2, bsize3, bsize4;
	int c;
	void *buf, *buf2;
	bptree_node *n, *n2;
	n = create_new_empty_bptree_node();
	n->key_count = 2;
	uuid_generate_random(n->self_key);
	n->leaf = 0;
	uuid_generate_random(n->children[0]);
	uuid_generate_random(n->children[1]);
	uuid_generate_random(n->children[2]);
	n->key_sizes[0] = 5;
	n->key_sizes[1] = 7;
	n->keys[0] = (unsigned char *) malloc(5);
	n->keys[1] = (unsigned char *) malloc(7);
	strncpy((char *)n->keys[0], "aaaa",4);
	strncpy((char *)n->keys[1], "aaaaaa", 6);
	
	n->value_sizes[0] = 3;
	n->value_sizes[1] = 9;
	n->values[0] = (unsigned char *) malloc(3);
	n->values[1] = (unsigned char *) malloc(9);
	strncpy((char *)n->values[0] , "ccc", 3);
	strncpy((char *)n->values[1] , "dddddddd", 10);
	buf = marshall_bptree_node(n, &bsize1);
	EXPECT_FALSE(buf == NULL);
	n2 = unmarshall_bptree_node(buf, bsize1, &bsize3);
	EXPECT_FALSE(n2 == NULL);
	buf2 = marshall_bptree_node(n2, &bsize2);
	EXPECT_FALSE(buf2 == NULL);
	c = memcmp(buf, buf2, bsize1);
	// The buffers buf and buf2 should now be identical
	EXPECT_TRUE (bsize1 > 0);
	EXPECT_TRUE (bsize1 == bsize2);
	EXPECT_TRUE (c == 0);

}

TEST_F(BptreeCoreTest, MetaNodeSerDe) {
	size_t bsize1, bsize2;
	int c;
	void *buf, *buf2;
	bptree_meta_node m, *m2;
	memset(&m, 0, sizeof(bptree_meta_node));
	m.execution_id = 567;
	uuid_generate_random(m.root_key);
	m.bpt_id = 123;
	
	buf = marshall_bptree_meta_node(&m, &bsize1);
	EXPECT_FALSE(buf == NULL);
	m2 = unmarshall_bptree_meta_node(buf, bsize1);
	EXPECT_FALSE(m2 == NULL);
	buf2 = marshall_bptree_meta_node(m2, &bsize2);
	EXPECT_FALSE(m2 == NULL);
	c = memcmp(buf, buf2, bsize1);
	// The buffers buf and buf2 should now be identical
	EXPECT_TRUE (bsize1 > 0);
	EXPECT_TRUE(bsize1 == bsize2);
	EXPECT_TRUE(c == 0);

}

TEST_F(BptreeCoreTest, FindKeyInNode) {
	int rv, num_elem = 7;
	if(BPTREE_NODE_SIZE < num_elem) {
		printf("Cannot run test with BPTREE_NODE_SIZE %d, deg %d\n",
		       BPTREE_NODE_SIZE, BPTREE_MIN_DEGREE);
		return;
	}
	rv = bptree_initialize_bpt_session(bps, 1000, BPTREE_OPEN_OVERWRITE, 
		BPTREE_INSERT_UNIQUE_KEY);
	bptree_set_num_fields(bps, 1);
	bptree_set_field_info(bps, 0, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32,
		int32cmp);

	bptree_node *n = makeRandomBptreeNode(bps, num_elem);
	bptree_key_val kv[3];
	int keys[3];
	keys[0] = 0;
	keys[1] = 10 * (num_elem / 2);
	keys[2] = 10 * (num_elem - 1);
	
	char v[5] = "cccc"; 
	for (int i=0; i < 3; i++) {
		kv[i].k = (unsigned char *)&keys[i];
		kv[i].v = (unsigned char *)v;
		kv[i].ksize = sizeof(int);
		kv[i].vsize = sizeof(int);
	}

	verifyKvInNode(n, kv, 3);
}


TEST_F(BptreeCoreTest, FindMultiLevelKeyInNode) {
	int rv, num_elem = 7;
	
	int f = 4, s = 2;
	if(BPTREE_NODE_SIZE < f * s) {
		printf("Cannot run test with BPTREE_NODE_SIZE %d, deg %d\n",
		       BPTREE_NODE_SIZE, BPTREE_MIN_DEGREE);
		return;
	}
	rv = bptree_initialize_bpt_session(bps, 1000, BPTREE_OPEN_OVERWRITE,
		BPTREE_INSERT_UNIQUE_KEY);
	bptree_set_num_fields(bps, 2);
	bptree_set_field_info(bps, 0, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32,
		int32cmp);
	bptree_set_field_info(bps, 1, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32,
		int32cmp);

	bptree_node *n = makeRandomMultiBptreeNode(bps,f, s);
	bptree_key_val kv[10];
	char v[5] = "cccc"; 
	int c =0;
	//printf("Checking: ");
	for (int i=0; i < f; i++) {
		for(int j=0; j < s; j++) {
			int k = i * 10;
			int l = j * 10;
			unsigned char *_k = (unsigned char *) malloc(50); // leak, my pretty!
			memcpy(_k, &k, sizeof(int));
			memcpy(_k+sizeof(int), &l, sizeof(int));
			//printf(" (%d,%d) -> %s, ",k,l,v);
			kv[c].k = _k;
			kv[c].v = (unsigned char *)v;
			kv[c].ksize = sizeof(int)*2;
			kv[c].vsize = sizeof(int);
			c++;
		}
	}
	//printf("\n");
	verifyKvInNode(n, kv, f*s);
}


TEST_F(BptreeCoreTest, FindPartialKeyInNode) {
	int rv, num_elem = 7;
	
	int f = 4, s = 2;
	if(BPTREE_NODE_SIZE < f * s) {
		printf("Cannot run test with BPTREE_NODE_SIZE %d, deg %d\n",
		       BPTREE_NODE_SIZE, BPTREE_MIN_DEGREE);
		return;
	}
	rv = bptree_initialize_bpt_session(bps, 1000, BPTREE_OPEN_OVERWRITE, 
		BPTREE_INSERT_UNIQUE_KEY);
	bptree_set_num_fields(bps, 2);
	bptree_set_field_info(bps, 0, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32,
		int32cmp);
	bptree_set_field_info(bps, 1, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32,
		int32cmp);

	bptree_node *n = makeRandomMultiBptreeNode(bps,f, s);
	bptree_key_val kv[5];
	char v[5] = "cccc"; 
	int c =0;
	// A set of partial keys to search against
	for (int i=0; i < f; i++) {
		int k = i * 10;
		unsigned char *_k = (unsigned char *) malloc(50); // leak, my pretty!
		memcpy(_k, &k, sizeof(int));
		//printf(" (%d,NULL) -> %s, ",k,v);
		kv[c].k = _k;
		kv[c].v = (unsigned char *)v;
		kv[c].ksize = sizeof(int);
		kv[c].vsize = sizeof(int);
		c++;
	}
	//printf("\n");
	verifyKvInNode(n, kv, f);
}


TEST_F(BptreeCoreTest, FindMissingElement) {
	int rv, pos, num_elem = 7;
	if(BPTREE_NODE_SIZE < num_elem) {
		printf("Cannot run test with BPTREE_NODE_SIZE %d, deg %d\n",
		       BPTREE_NODE_SIZE, BPTREE_MIN_DEGREE);
		return;
	}
	rv = bptree_initialize_bpt_session(bps, 1000, BPTREE_OPEN_OVERWRITE,
		BPTREE_INSERT_UNIQUE_KEY );
	bptree_set_num_fields(bps, 1);
	bptree_set_field_info(bps, 0, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32,
		int32cmp);

	bptree_node *n = create_new_empty_bptree_node();
	bptree_key_val kv;
	int k, v;
	kv.k = (unsigned char *)&k;
	kv.ksize = sizeof(int);
	kv.v = (unsigned char *)&v;
	kv.vsize = sizeof(int);
	k = 1;
	v = 1000;
	copy_key_val_to_node(n, &kv, 0);
	EXPECT_EQ(k, *(int *)n->keys[0]);
	k = 5; 
	copy_key_val_to_node(n, &kv, 1);
	EXPECT_EQ(k, *(int *)n->keys[1]);
	k = 10; 
	copy_key_val_to_node(n, &kv, 2);
	EXPECT_EQ(k, *(int *)n->keys[2]);
	k = 15; 
	copy_key_val_to_node(n, &kv, 3);
	EXPECT_EQ(k, *(int *)n->keys[3]);
	
	rv = is_node_ordered(bps, n);
	EXPECT_EQ(rv, 0);
	
	k = 7; 
	pos = find_position_in_node(bps, n, &kv, &rv);
	EXPECT_EQ(BPTREE_OP_KEY_NOT_FOUND , rv);
	EXPECT_EQ(2, pos);
	
	k = -1;
	pos = find_position_in_node(bps, n, &kv, &rv);
	EXPECT_EQ(BPTREE_OP_KEY_NOT_FOUND , rv);
	EXPECT_EQ(0, pos);
	
	k = 16; 
	pos = find_position_in_node(bps, n, &kv, &rv);
	EXPECT_EQ(BPTREE_OP_KEY_NOT_FOUND , rv);
	EXPECT_EQ(4, pos);
	
}
TEST_F(BptreeCoreTest, ReadNodeMock) {
	int rv, num_elem = 5;
	
	if(BPTREE_NODE_SIZE < num_elem) {
		printf("Cannot run test with BPTREE_NODE_SIZE %d, deg %d\n",
		       BPTREE_NODE_SIZE, BPTREE_MIN_DEGREE);
		return;
	}
	rv = bptree_initialize_bpt_session(bps, 1000, BPTREE_OPEN_OVERWRITE,
		BPTREE_INSERT_UNIQUE_KEY );
	bptree_set_num_fields(bps, 1);
	bptree_set_field_info(bps, 0, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32,
		int32cmp);

	bptree_node *n = makeRandomBptreeNode(bps, 5);
	rv = write_node(bps,n);  
	EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	
	bptree_node *n2 = read_node(bps, n->self_key, &rv); 
	EXPECT_EQ(rv, BPTREE_OP_NODE_FOUND);
	EXPECT_TRUE(uuid_compare(n->self_key, n2->self_key) == 0);
	EXPECT_EQ(n->key_count, n2->key_count);
}

TEST_F(BptreeIntBasedTreeTest, DeleteElement) {
	int rv, pos, num_elem = 7;
	if(BPTREE_NODE_SIZE < num_elem) {
		printf("Cannot run test with BPTREE_NODE_SIZE %d, deg %d\n",
		       BPTREE_NODE_SIZE, BPTREE_MIN_DEGREE);
		return;
	}
	bptree_node *n = makeRandomBptreeNode(bps, num_elem);
	
	EXPECT_EQ(n->key_count, num_elem);
	// Out of bounds
	delete_key_from_node(n, num_elem);
	EXPECT_EQ(n->key_count, num_elem);
	EXPECT_TRUE(is_node_sane(n) == 0);
	// Border case
	delete_key_from_node(n, num_elem-1);
	EXPECT_EQ(n->key_count, num_elem-1);
	EXPECT_TRUE(is_node_sane(n) == 0);
	// Middle
	delete_key_from_node(n, num_elem/2);
	EXPECT_EQ(n->key_count, num_elem-2);
	EXPECT_TRUE(is_node_sane(n) == 0);
	// First
	delete_key_from_node(n, 0);
	EXPECT_EQ(n->key_count, num_elem-3);
	EXPECT_TRUE(is_node_sane(n) == 0);
	
	// Final node sanity checking
	EXPECT_TRUE(is_node_ordered(bps, n) == 0);
	
}

TEST_F(BptreeIntBasedTreeTest, DeleteFromNonTrivialTree) {
	int n = BPTREE_NODE_SIZE * 5;
	for(int i= 1; i <= n; i++) {
		k = i*100;
		v = i*10000;
		rv = bptree_insert(bps, &k, sizeof(k), &v, sizeof(v));
		EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	}
	
	//for(int i= n; i >= 1; i--) {
	for(int i= 1; i <= n; i++) {
		k = i*100;
		v = i*10000;
		rv = bptree_delete(bps, &k, sizeof(k), &v, sizeof(v)); 
		EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
	}
	
	rv = bptree_index_first(bps, &k, &ksize, &v, &vsize); 
	EXPECT_EQ(rv, BPTREE_OP_EOF);
}

TEST_F(BptreeIntBasedTreeTest, DeleteFromNonTrivialTreeWithDupesForward) {
	
	bps = mockBptreeSessionCreate();
	createNewMockSession(1000,
			     BPTREE_OPEN_OVERWRITE,
			     BPTREE_INSERT_ALLOW_DUPES);
	
	int n = BPTREE_NODE_SIZE * 3;
	for(int i= 1; i <= n; i++) {
		k = i*100;
		for (int j = 1; j < 3; j++) {
			v = i*10000 + j;
			rv = bptree_insert(bps, &k, sizeof(k), &v, sizeof(v));
			EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
		}
	}
	bptree_debug(bps, BPTREE_DEBUG_DUMP_RECURSIVELY, nn);
	bptree_debug(bps, BPTREE_DEBUG_DUMP_GRAPHVIZ, nn);
	
	for(int i= 1; i <= n; i++) {
		k = i*100;
		for (int j = 1; j < 3; j++) {
			v = i*10000 + j;
			rv = bptree_delete(bps, &k, sizeof(k), &v, sizeof(v)); 
			EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
		}
	}
	
	rv = bptree_index_first(bps, &k, &ksize, &v, &vsize); 
	EXPECT_EQ(rv, BPTREE_OP_EOF);
	
}

TEST_F(BptreeIntBasedTreeTest, DeleteFromNonTrivialTreeWithDupesReverse) {
	
	bps = mockBptreeSessionCreate();
	createNewMockSession(1000,
			     BPTREE_OPEN_OVERWRITE,
			     BPTREE_INSERT_ALLOW_DUPES);
	
	int n = BPTREE_NODE_SIZE * 5;
	for(int i= 1; i <= n; i++) {
		k = i*100;
		for (int j = 1; j < 3; j++) {
			v = i*10000 + j;
			rv = bptree_insert(bps, &k, sizeof(k), &v, sizeof(v));
			EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
		}
	}
	uuid_t nn;
	bptree_debug(bps, BPTREE_DEBUG_DUMP_RECURSIVELY, nn);
	bptree_debug(bps, BPTREE_DEBUG_DUMP_GRAPHVIZ, nn);
	
	for(int i= n; i >= 1; i--) {
		k = i*100;
		for (int j = 1; j < 3; j++) {
			v = i*10000 + j;
			rv = bptree_delete(bps, &k, sizeof(k), &v, sizeof(v)); 
			EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
		}
	}
	
	rv = bptree_index_first(bps, &k, &ksize, &v, &vsize); 
	EXPECT_EQ(rv, BPTREE_OP_EOF);
	
}

TEST_F(BptreeIntBasedTreeTest, DeleteFromTrivialTree) {
	// Inserting more than nodesize ensures that at least one split happens
	int n = BPTREE_NODE_SIZE / 2;
	for(int i= 1; i <= n; i++) {
		k = i*100;
		v = i*10000;
		rv = bptree_insert(bps, &k, sizeof(k), &v, sizeof(v));
		EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	}
	for(int i= 1; i <= n; i++) {
		k = i*100;
		rv = bptree_delete(bps, &k, sizeof(k), &v, sizeof(v)); 
		EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
	}
	
	rv = bptree_index_first(bps, &k, &ksize, &v, &vsize); 
	EXPECT_EQ(rv, BPTREE_OP_EOF);
}

// STUBS: Add similar test cases for concat and split
TEST_F(BptreeIntBasedTreeTest, ConcatUnderflowLeaf) {
	
	bptree_node *cl, *cr, *p;

	createMiniTree(&p, &cl, BPTREE_NODE_MIN_SIZE - 1, 
			      &cr, BPTREE_NODE_MIN_SIZE , false);
	
	if (DBUG) {
		printf("BEFORE concatenate:\n");
		dump_node_info(bps, p);
		dump_node_info(bps, cl);
		dump_node_info(bps, cr);
	}
	
	concatenate_nodes(p,cl,cr,0);
	
	if (DBUG) {
		printf("AFTER concatenate:\n");
		dump_node_info(bps, p);
		dump_node_info(bps, cl);
		dump_node_info(bps, cr);
	}
	
	
	// Concat should have 'deleted' both parent and right node
	EXPECT_EQ(p->key_count, 0);
	EXPECT_EQ(cr->key_count, 0);
	EXPECT_TRUE(p->node_active == 0);
	EXPECT_TRUE(cr->node_active == 0);
	EXPECT_EQ(is_node_ordered(bps,cl), 0);
	// Since this is a leaf, the split key was already there
	EXPECT_EQ(cl->key_count, BPTREE_NODE_MIN_SIZE * 2 - 1);
}

TEST_F(BptreeIntBasedTreeTest, ConcatUnderflowNonLeaf) {
	
	bptree_node *cl, *cr, *p;

	createMiniTree(&p, &cl, BPTREE_NODE_MIN_SIZE - 1, 
			      &cr, BPTREE_NODE_MIN_SIZE, true);
	
	if (DBUG) {
		printf("BEFORE concatenate:\n");
		dump_node_info(bps, p);
		dump_node_info(bps, cl);
		dump_node_info(bps, cr);
	}
	
	concatenate_nodes(p,cl,cr,0);
	
	if (DBUG) {
		printf("AFTER concatenate:\n");
		dump_node_info(bps, p);
		dump_node_info(bps, cl);
		dump_node_info(bps, cr);
	}
	
	
	// Concat should have 'deleted' both parent and right node
	EXPECT_EQ(p->key_count, 0);
	EXPECT_EQ(cr->key_count, 0);
	EXPECT_TRUE(p->node_active == 0);
	EXPECT_TRUE(cr->node_active == 0);
	EXPECT_EQ(is_node_ordered(bps,cl), 0);
	EXPECT_EQ(cl->key_count, BPTREE_NODE_MIN_SIZE * 2);
	
}
	
TEST_F(BptreeIntBasedTreeTest, SplitOverflowNonLeaf) {
	
	bptree_node *cl, *cr1, *cr2, *p;

	createMiniTree(&p, &cl, BPTREE_NODE_SIZE, 
			      &cr2, BPTREE_NODE_MIN_SIZE, true);
	
	// Make a copy of all children of the full node, and make sure
	// they were distributed properly across the two nodes
	uuid_t children[BPTREE_NODE_SIZE+1];
	for (int i=0; i<= BPTREE_NODE_SIZE; i++) {
		uuid_copy(children[i], cl->children[i]);
	}
	
	cr1 = create_new_bptree_node(bps);
	if (DBUG) {
		printf("BEFORE split:\n");
		dump_node_info(bps, p);
		dump_node_info(bps, cl);
		dump_node_info(bps, cr2);
	}
	
	bptree_split_child(bps, p, 0, cl, cr1);
	
	if (DBUG) {
		printf("AFTER split:\n");
		dump_node_info(bps, p);
		dump_node_info(bps, cl);
		dump_node_info(bps, cr1);
		dump_node_info(bps, cr2);
	}
	
	for (int i=0; i<= BPTREE_NODE_MIN_SIZE; i++) {
		EXPECT_EQ(uuid_compare(cl->children[i],children[i]), 0);
		EXPECT_EQ(uuid_compare(cr1->children[i],
				       children[i+BPTREE_NODE_MIN_SIZE+1]), 0);
	}
	
	EXPECT_EQ(p->key_count, 2);
	EXPECT_EQ(cl->key_count, BPTREE_NODE_MIN_SIZE);
	EXPECT_EQ(cr1->key_count, BPTREE_NODE_MIN_SIZE);
	EXPECT_EQ(cr2->key_count, BPTREE_NODE_MIN_SIZE);
	
	EXPECT_EQ(are_split_cells_valid(bps, p, 0, cl, cr1), 0);
	
	// Ensure links were maintained properly
	EXPECT_EQ(uuid_compare(p->children[0], cl->self_key), 0);
	EXPECT_EQ(uuid_compare(p->children[1], cr1->self_key), 0);
	EXPECT_EQ(uuid_compare(p->children[2], cr2->self_key), 0);
}

TEST_F(BptreeIntBasedTreeTest, SplitOverflowLeaf) {
	bptree_node *cl, *cr1, *cr2, *p;

	createMiniTree(&p, &cl, BPTREE_NODE_SIZE, 
			      &cr2, BPTREE_NODE_MIN_SIZE, false);
	
	cr1 = create_new_bptree_node(bps);
	if (DBUG) {
		printf("BEFORE split:\n");
		dump_node_info(bps, p);
		dump_node_info(bps, cl);
		dump_node_info(bps, cr2);
	}
	
	bptree_split_child(bps, p, 0, cl, cr1);
	
	if (DBUG) {
		printf("AFTER split:\n");
		dump_node_info(bps, p);
		dump_node_info(bps, cl);
		dump_node_info(bps, cr1);
		dump_node_info(bps, cr2);
	}
	
	EXPECT_TRUE(p->key_count == 2);
	EXPECT_EQ(cl->key_count, BPTREE_NODE_MIN_SIZE);
	EXPECT_EQ(cr1->key_count, BPTREE_NODE_MIN_SIZE + 1);
	EXPECT_EQ(cr2->key_count, BPTREE_NODE_MIN_SIZE);
	
	EXPECT_EQ(are_split_cells_valid(bps, p, 0, cl, cr1), 0);
	
	// Ensure links were maintained properly
	EXPECT_EQ(uuid_compare(p->children[0], cl->self_key), 0);
	EXPECT_EQ(uuid_compare(p->children[1], cr1->self_key), 0);
	EXPECT_EQ(uuid_compare(p->children[2], cr2->self_key), 0);
	
	EXPECT_EQ(uuid_compare(cl->next_node, cr1->self_key), 0);
	EXPECT_EQ(uuid_compare(cr1->next_node, cr2->self_key), 0);
	
}

TEST_F(BptreeIntBasedTreeTest, RedistUnderflowNonLeaf) {
	bptree_node *cl, *cr, *p;

	///////////////////////////////////////////////////////////////
	// Right-to-left transfer 
	createMiniTree(&p, &cl, BPTREE_NODE_MIN_SIZE - 1, 
			      &cr, BPTREE_NODE_MIN_SIZE + 1, true);
	
	redistribute_keys(p,cl,cr,0);
	
	EXPECT_TRUE(p->key_count == 1);
	EXPECT_TRUE(cl->key_count == BPTREE_NODE_MIN_SIZE);
	EXPECT_TRUE(cr->key_count == BPTREE_NODE_MIN_SIZE);
		
	miniTreeSanityChecks(p,cl,cr);
	
	///////////////////////////////////////////////////////////////
	// Left-to-right transfer 
	createMiniTree(&p, &cl, BPTREE_NODE_MIN_SIZE + 1, 
			      &cr, BPTREE_NODE_MIN_SIZE - 1, true);
	
	if (DBUG) {
		printf("BEFORE redist:\n");
		dump_node_info(bps, p);
		dump_node_info(bps, cl);
		dump_node_info(bps, cr);
	}
	
	redistribute_keys(p,cl,cr,0);
	
	if (DBUG) {
		printf("\n\nAFTER redist:\n");
		dump_node_info(bps, p);
		dump_node_info(bps, cl);
		dump_node_info(bps, cr);
	}
	
	EXPECT_TRUE(p->key_count == 1);
	EXPECT_TRUE(cl->key_count == BPTREE_NODE_MIN_SIZE);
	EXPECT_TRUE(cr->key_count == BPTREE_NODE_MIN_SIZE);
		
		
	miniTreeSanityChecks(p,cl,cr);
}
	
TEST_F(BptreeIntBasedTreeTest, RedistUnderflowLeaf) {
	bptree_node *cl, *cr, *p;

	///////////////////////////////////////////////////////////////
	// Right-to-left transfer 
	createMiniTree(&p, &cl, BPTREE_NODE_MIN_SIZE - 1, 
			      &cr, BPTREE_NODE_MIN_SIZE + 1, false);
	
	if (DBUG) {
		printf("BEFORE redist:\n");
		dump_node_info(bps, p);
		dump_node_info(bps, cl);
		dump_node_info(bps, cr);
	}
	
	redistribute_keys(p,cl,cr,0);
	
	if (DBUG) {
		printf("\n\nAFTER redist:\n");
		dump_node_info(bps, p);
		dump_node_info(bps, cl);
		dump_node_info(bps, cr);
	}
	
	EXPECT_TRUE(p->key_count == 1);
	EXPECT_TRUE(cl->key_count == BPTREE_NODE_MIN_SIZE);
	EXPECT_TRUE(cr->key_count == BPTREE_NODE_MIN_SIZE);
		
	miniTreeSanityChecks(p,cl,cr);
		
	///////////////////////////////////////////////////////////////
	// Left-to-right transfer 
	createMiniTree(&p, &cl, BPTREE_NODE_MIN_SIZE + 1, 
			      &cr, BPTREE_NODE_MIN_SIZE - 1, false);
	
	redistribute_keys(p,cl,cr,0);
	
	EXPECT_TRUE(p->key_count == 1);
	EXPECT_TRUE(cl->key_count == BPTREE_NODE_MIN_SIZE);
	EXPECT_TRUE(cr->key_count == BPTREE_NODE_MIN_SIZE);
		
	miniTreeSanityChecks(p,cl,cr);
}
	
TEST_F(BptreeIntBasedTreeTest, MoreThanOneNodeInsert) {
	// Inserting more than nodesize ensures that at least one split happens
	int n = 5 * BPTREE_NODE_SIZE;
	for(int i= 1; i <= n; i++) {
		k = i*100;
		v = i*10000;
		rv = bptree_insert(bps, &k, sizeof(k), &v, sizeof(v));
		EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	}
	
	for(int i= 1; i <= n; i++) {
		k = i*100;
		rv = bptree_search(bps, &k, sizeof(k), &v, &vsize); 
		EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
		EXPECT_EQ(vsize, sizeof(v));
		EXPECT_EQ(v, i*10000);
	}
}

TEST_F(BptreeIntBasedTreeTest, InsertDupe) {
	k = 1000;
	v = 1234;
	rv = bptree_insert(bps, &k, sizeof(k), &v, sizeof(v));
	EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	rv = bptree_insert(bps, &k, sizeof(k), &v, sizeof(v));
	EXPECT_EQ(rv, BPTREE_ERR_DUPLICATE_KEY_INSERTED);
	v = 0;
	rv = bptree_search(bps, &k, sizeof(k), &v, &vsize); 
	EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
	EXPECT_EQ(v, 1234);
	EXPECT_EQ(vsize, sizeof(v));
}
