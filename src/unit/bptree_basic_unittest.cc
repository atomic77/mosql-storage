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
	
	bptree_node * make_random_multi_bptree_node(bptree_session *bps, 
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
		assert(is_cell_ordered(bps, n));
		
		return n;
		
	}
	
	bptree_node * make_random_bptree_node(bptree_session *bps, int num_elem) {
		if (num_elem >= BPTREE_NODE_SIZE) return NULL;
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
		assert(is_cell_ordered(bps, n));
		
		return n;
		
	}
	
	bptree_session * mock_bptree_session_create() {
		
		bptree_session *bps = (bptree_session *) malloc(sizeof(bptree_session));
		memset(bps, 0, sizeof(bps));
		
		bps->bpt_id = 1000;
		bps->tapioca_client_id = 1;
		bps->insert_flags = BPTREE_INSERT_UNIQUE_KEY;
		bps->t = transaction_new();
		return bps;
	}
	
	/*@ Assumes that the k-vs in kv[] have been 'inserted' into node already
	 and were assigned with *v as the value*/
	void verify_kv_in_node(bptree_node *x, bptree_key_val *kv, int keys)
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
		bps = mock_bptree_session_create();
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
		bps = mock_bptree_session_create();
		
		bptree_initialize_bpt_session(bps, 1000, 
			BPTREE_OPEN_OVERWRITE, BPTREE_INSERT_UNIQUE_KEY );
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
	
	ASSERT_GE(BPTREE_NODE_SIZE, 7);
	rv = bptree_initialize_bpt_session(bps, 1000, BPTREE_OPEN_OVERWRITE, 
		BPTREE_INSERT_UNIQUE_KEY);
	bptree_set_num_fields(bps, 1);
	bptree_set_field_info(bps, 0, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32,
		int32cmp);

	bptree_node *n = make_random_bptree_node(bps, num_elem);
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

	verify_kv_in_node(n, kv, 3);
}


TEST_F(BptreeCoreTest, FindMultiLevelKeyInNode) {
	int rv, num_elem = 7;
	
	rv = bptree_initialize_bpt_session(bps, 1000, BPTREE_OPEN_OVERWRITE,
		BPTREE_INSERT_UNIQUE_KEY);
	bptree_set_num_fields(bps, 2);
	bptree_set_field_info(bps, 0, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32,
		int32cmp);
	bptree_set_field_info(bps, 1, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32,
		int32cmp);

	int f = 5, s = 2;
	ASSERT_GE(BPTREE_NODE_SIZE, f * s);
	bptree_node *n = make_random_multi_bptree_node(bps,f, s);
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
	verify_kv_in_node(n, kv, f*s);
}


TEST_F(BptreeCoreTest, FindPartialKeyInNode) {
	int rv, num_elem = 7;
	
	ASSERT_GE(BPTREE_NODE_SIZE, 7);
	rv = bptree_initialize_bpt_session(bps, 1000, BPTREE_OPEN_OVERWRITE, 
		BPTREE_INSERT_UNIQUE_KEY);
	bptree_set_num_fields(bps, 2);
	bptree_set_field_info(bps, 0, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32,
		int32cmp);
	bptree_set_field_info(bps, 1, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32,
		int32cmp);

	int f = 5, s = 2;
	bptree_node *n = make_random_multi_bptree_node(bps,f, s);
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
	verify_kv_in_node(n, kv, f);
}


TEST_F(BptreeCoreTest, FindMissingElement) {
	int rv, pos, num_elem = 7;
	ASSERT_GE(BPTREE_NODE_SIZE, 7);
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
	
	rv = is_cell_ordered(bps, n);
	EXPECT_TRUE(rv != 0);
	
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
	int rv;
	rv = bptree_initialize_bpt_session(bps, 1000, BPTREE_OPEN_OVERWRITE,
		BPTREE_INSERT_UNIQUE_KEY );
	bptree_set_num_fields(bps, 1);
	bptree_set_field_info(bps, 0, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32,
		int32cmp);

	bptree_node *n = make_random_bptree_node(bps, 5);
	rv = write_node(bps,n);  
	EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	
	bptree_node *n2 = read_node(bps, n->self_key, &rv); 
	EXPECT_EQ(rv, BPTREE_OP_NODE_FOUND);
	EXPECT_TRUE(uuid_compare(n->self_key, n2->self_key) == 0);
	EXPECT_EQ(n->key_count, n2->key_count);
}

TEST_F(BptreeIntBasedTreeTest, DeleteElement) {
	int rv, pos, num_elem = 7;
	bptree_node *n = make_random_bptree_node(bps, num_elem);
	
	EXPECT_EQ(n->key_count, num_elem);
	// Out of bounds
	delete_key_from_node(n, num_elem);
	EXPECT_EQ(n->key_count, num_elem);
	EXPECT_TRUE(is_node_sane(n));
	// Border case
	delete_key_from_node(n, num_elem-1);
	EXPECT_EQ(n->key_count, num_elem-1);
	EXPECT_TRUE(is_node_sane(n));
	// Middle
	delete_key_from_node(n, num_elem/2);
	EXPECT_EQ(n->key_count, num_elem-2);
	EXPECT_TRUE(is_node_sane(n));
	// First
	delete_key_from_node(n, 0);
	EXPECT_EQ(n->key_count, num_elem-3);
	EXPECT_TRUE(is_node_sane(n));
	
	// Final node sanity checking
	EXPECT_TRUE(is_cell_ordered(bps, n));
	EXPECT_TRUE(are_key_and_value_sizes_valid(n));
	
}

TEST_F(BptreeIntBasedTreeTest, DeleteFromHeightTwoTree) {
	// Inserting more than nodesize ensures that at least one split happens
	int n = BPTREE_NODE_SIZE * 2;
	for(int i= 1; i <= n; i++) {
		k = i*100;
		v = i*10000;
		rv = bptree_insert(bps, &k, sizeof(k), &v, sizeof(v));
		EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	}
	
	for(int i= n; i >= 1; i--) {
		k = i*100;
		v = i*10000;
		rv = bptree_delete(bps, &k, sizeof(k), &v, sizeof(v)); 
		EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
	}
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

// TODO Write basic unit tests for:
// find position of elements that don't exist
// shift_bptree_node_elements
// shift_bptree_node_children
// move_bptree_node_element
// copy_key_val_to_node
// compar functions



