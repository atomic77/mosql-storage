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

#include "bptree_core.h"

TEST_F(BptreeCoreTest, NodeSerDe) {
	size_t bsize1, bsize2, bsize3, bsize4;
	int c;
	void *buf, *buf2;
	bptree_node *n, *n2;
	n = bpnode_new();
	bpnode_set_leaf(n, 0); 
	uuid_t u;
	uuid_generate_random(u);
	bptree_node *c0 = bpnode_new();
	bptree_node *c1 = bpnode_new();
	bptree_node *c2 = bpnode_new();
	bpnode_set_child(n, 0, c0);
	bpnode_set_child(n, 1, c1);
	bpnode_set_child(n, 2, c2);
	bptree_key_val kv1, kv2;
	char k1[3], k2[9], v1[5], v2[7];
	kv1.ksize = 5;
	kv2.ksize = 7;
	kv1.k = (unsigned char *) malloc(5);
	kv2.k = (unsigned char *) malloc(7);
	strncpy((char *)kv1.k, "aaaa",4);
	strncpy((char *)kv2.k, "aaaaaa", 6);
	
	kv1.vsize = 3;
	kv2.vsize = 9;
	kv1.v = (unsigned char *) malloc(3);
	kv2.v = (unsigned char *) malloc(9);
	strncpy((char *)kv1.v , "ccc", 3);
	strncpy((char *)kv2.v , "dddddddd", 8);
	copy_key_val_to_node(n, &kv1, 0);
	copy_key_val_to_node(n, &kv2, 1);
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

TEST_F(BptreeCoreTest, FindElementInNode) {
	int rv, k_pos, c_pos, num_elem = 4;
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

	bptree_node *n = bpnode_new();
	bptree_key_val kv;
	int k, v;
	kv.k = (unsigned char *)&k;
	kv.ksize = sizeof(int);
	kv.v = (unsigned char *)&v;
	kv.vsize = sizeof(int);
	k = 1;
	v = 1000;
	copy_key_val_to_node(n, &kv, 0);
	EXPECT_EQ(k, *(int *)bpnode_get_key(n, 0));
	k = 5; 
	copy_key_val_to_node(n, &kv, 1);
	EXPECT_EQ(k, *(int *)bpnode_get_key(n, 1));
	k = 10; 
	copy_key_val_to_node(n, &kv, 2);
	EXPECT_EQ(k, *(int *)bpnode_get_key(n, 2));
	k = 15; 
	copy_key_val_to_node(n, &kv, 3);
	EXPECT_EQ(k, *(int *)bpnode_get_key(n, 3));
	
	dump_node_info(bps, n);
	rv = is_node_ordered(bps, n);
	EXPECT_EQ(rv, 0);
	
	k = 7; 
	rv = find_position_in_node(bps, n, &kv, &k_pos, &c_pos);
	EXPECT_EQ(BPTREE_OP_KEY_NOT_FOUND , rv);
	EXPECT_EQ(2, k_pos);
	EXPECT_EQ(2, c_pos);
	
	k = -1;
	rv = find_position_in_node(bps, n, &kv, &k_pos, &c_pos);
	EXPECT_EQ(BPTREE_OP_KEY_NOT_FOUND , rv);
	EXPECT_EQ(0, k_pos);
	EXPECT_EQ(0, c_pos);
	
	k = 1;
	rv = find_position_in_node(bps, n, &kv, &k_pos, &c_pos);
	EXPECT_EQ(BPTREE_OP_KEY_FOUND , rv);
	EXPECT_EQ(0, k_pos);
	EXPECT_EQ(1, c_pos);
	
	k = 15; 
	rv = find_position_in_node(bps, n, &kv, &k_pos, &c_pos);
	EXPECT_EQ(BPTREE_OP_KEY_FOUND , rv);
	EXPECT_EQ(3, k_pos);
	EXPECT_EQ(4, c_pos);
	
	k = 16; 
	rv = find_position_in_node(bps, n, &kv, &k_pos, &c_pos);
	EXPECT_EQ(BPTREE_OP_KEY_NOT_FOUND , rv);
	EXPECT_EQ(4, k_pos);
	EXPECT_EQ(4, c_pos);
	
}
// Helper to clean up the find partial element test case
inline void write_pair_to_buf(unsigned char *dbuf, int i1, int i2){
	memcpy(dbuf, &i1, sizeof(int));
	memcpy(dbuf+sizeof(int), &i2, sizeof(int));
}
TEST_F(BptreeCoreTest, FindPartialElementInNode) {
	int rv, k_pos, c_pos, num_elem = 5;
	if(BPTREE_NODE_SIZE < num_elem) {
		printf("Cannot run test with BPTREE_NODE_SIZE %d, deg %d\n",
		       BPTREE_NODE_SIZE, BPTREE_MIN_DEGREE);
		return;
	}
	rv = bptree_initialize_bpt_session(bps, 1000, BPTREE_OPEN_OVERWRITE,
		BPTREE_INSERT_UNIQUE_KEY );
	bptree_set_num_fields(bps, 2);
	bptree_set_field_info(bps, 0, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32,
		int32cmp);
	bptree_set_field_info(bps, 1, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32,
		int32cmp);

	bptree_node *n = bpnode_new();
	bptree_key_val kv;
	int k1, k2, v;
	unsigned char kbuf[sizeof(int)*2];
	kv.k = kbuf;
	kv.ksize = sizeof(int)*2;
	kv.v = (unsigned char *)&v;
	kv.vsize = sizeof(int);
	v = 1000;
	
	write_pair_to_buf(kbuf, 100, 1);
	copy_key_val_to_node(n, &kv, 0);
	
	write_pair_to_buf(kbuf, 100, 2);
	copy_key_val_to_node(n, &kv, 1);
	
	write_pair_to_buf(kbuf, 200, 1);
	copy_key_val_to_node(n, &kv, 2);
	
	write_pair_to_buf(kbuf, 200, 2);
	copy_key_val_to_node(n, &kv, 3);
	
	write_pair_to_buf(kbuf, 300, 1);
	copy_key_val_to_node(n, &kv, 4);
	
	dump_node_info(bps, n);
	rv = is_node_ordered(bps, n);
	EXPECT_EQ(rv, 0);
	
	// Partial matches should favour the left side
	kv.k = (unsigned char *)&k1;
	kv.ksize = sizeof(int);
	k1 = 100; 
	rv = find_position_in_node(bps, n, &kv, &k_pos, &c_pos);
	EXPECT_EQ(BPTREE_OP_KEY_FOUND , rv);
	EXPECT_EQ(0, k_pos);
	EXPECT_EQ(0, c_pos);
	
	k1 = 150;
	rv = find_position_in_node(bps, n, &kv, &k_pos, &c_pos);
	EXPECT_EQ(BPTREE_OP_KEY_NOT_FOUND , rv);
	EXPECT_EQ(2, k_pos);
	EXPECT_EQ(2, c_pos);
	
	k1 = 200;
	rv = find_position_in_node(bps, n, &kv, &k_pos, &c_pos);
	EXPECT_EQ(BPTREE_OP_KEY_FOUND , rv);
	EXPECT_EQ(2, k_pos);
	EXPECT_EQ(2, c_pos);
	
	k1 = 400;
	rv = find_position_in_node(bps, n, &kv, &k_pos, &c_pos);
	EXPECT_EQ(BPTREE_OP_KEY_NOT_FOUND , rv);
	EXPECT_EQ(5, k_pos);
	EXPECT_EQ(5, c_pos);
	
	// Test two-field exact matches
	kv.k = (unsigned char *)kbuf;
	kv.ksize = sizeof(int)*2;
	write_pair_to_buf(kbuf, 100,1);
	rv = find_position_in_node(bps, n, &kv, &k_pos, &c_pos);
	EXPECT_EQ(BPTREE_OP_KEY_FOUND , rv);
	EXPECT_EQ(0, k_pos);
	EXPECT_EQ(1, c_pos);
	
	write_pair_to_buf(kbuf, 150,2);
	rv = find_position_in_node(bps, n, &kv, &k_pos, &c_pos);
	EXPECT_EQ(BPTREE_OP_KEY_NOT_FOUND , rv);
	EXPECT_EQ(2, k_pos);
	EXPECT_EQ(2, c_pos);
	
	write_pair_to_buf(kbuf, 200,2);
	rv = find_position_in_node(bps, n, &kv, &k_pos, &c_pos);
	EXPECT_EQ(BPTREE_OP_KEY_FOUND , rv);
	EXPECT_EQ(3, k_pos);
	EXPECT_EQ(4, c_pos);
	
	write_pair_to_buf(kbuf, 400,1);
	rv = find_position_in_node(bps, n, &kv, &k_pos, &c_pos);
	EXPECT_EQ(BPTREE_OP_KEY_NOT_FOUND , rv);
	EXPECT_EQ(5, k_pos);
	EXPECT_EQ(5, c_pos);
	
	
	
	
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
	
	bptree_node *n2 = read_node(bps, bpnode_get_id(n), &rv); 
	EXPECT_EQ(rv, BPTREE_OP_NODE_FOUND);
	EXPECT_TRUE(bpnode_is_same(n, n2));
	EXPECT_EQ(bpnode_size(n), bpnode_size(n2));
}
