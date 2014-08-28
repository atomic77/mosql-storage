
#include "bptree_core.h"


class BptreeCoreIntTest : public BptreeCoreTest {
	
protected:
	int rv;
	int k, v, ksize, vsize;
	long unsigned int *seed;
	const gsl_rng_type *T;
	gsl_rng *rng;
	virtual void SetUp() {
		//system("cd ..; ./start.sh > /dev/null; cd unit");
		bps = mockBptreeSessionCreate();
		createNewMockSession();
		// Set up GSL random number generator
		T = gsl_rng_ranlxs2;
		rng = gsl_rng_alloc(T);
		// An ugly but easy way to get a decently random seed
		uuid_t uu_tmp;
		uuid_generate(uu_tmp);
		seed = (long unsigned int *)uu_tmp;
		//seed = 1234;
		gsl_rng_set(rng, *seed);
	
	}
	
	bptree_node * makeIncrementalBptreeNode(bptree_session *bps, 
						int start, int end,
						int num_elem) 
	{
		// Make a roughly spaced node with n elements
		if (num_elem > BPTREE_NODE_SIZE) return NULL;
		bptree_node *n = bpnode_new();
		
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
		int rv = is_node_ordered(bps, n) ;
		assert(rv == 0);
		
		return n;
		
	}
	
	void createMiniTree(bptree_node **parent, 
				   bptree_node **cleft, int l_sz,
				   bptree_node **cright, int r_sz,
				   bool hasChildren) 
	{
		
		bptree_node *p, *cl, *cr;
		p = bpnode_new();
		bpnode_set_leaf(p, 0);
		cl = makeIncrementalBptreeNode(bps, 100, 200, l_sz);
		cr = makeIncrementalBptreeNode(bps, 300, 400, r_sz);
		bptree_key_val *kv = bpnode_get_kv(cr, 0);
		if (hasChildren) {
			int a = 250;
			memcpy(kv->k, &a, sizeof(int));
		}
		copy_key_val_to_node(p, kv, 0);
		bpnode_set_child(p,0, cl);
		bpnode_set_child(p,1, cr);
		
		*parent = p;
		*cleft = cl;
		*cright = cr;
		
		if (hasChildren) {
			// Create fake children
			bpnode_set_leaf(cl, 0);
			bpnode_set_leaf(cr, 0);
			uuid_t u;
			for (int i =0; i <= bpnode_size(cl); i++) {
				uuid_generate_random(u);
				bpnode_set_child_id(cl, i, u);
			}
			for (int i =0; i <= bpnode_size(cr); i++) {
				uuid_generate_random(u);
				bpnode_set_child_id(cr, i, u);
			}
		} else {
			bpnode_set_next(cl, cr);
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
		
		EXPECT_EQ(uuid_compare(bpnode_get_child_id(p,0), bpnode_get_id(cl)),0);
		EXPECT_EQ(uuid_compare(bpnode_get_child_id(p,1), bpnode_get_id(cr)),0);
	
		if (!bpnode_is_leaf(cl)) {
			int c;
			/* Sanity checks will find simple corruptions, but 
			 * make sure we didn't do any funky copying or duplication*/
			for (int l =0; l < bpnode_size(cl); l++) {
				c = uuid_compare(bpnode_get_child_id(cl, l),
						 bpnode_get_child_id(cl, l+1));
				EXPECT_NE(c, 0);
			}
			
			for (int r =0; r < bpnode_size(cr); r++) {
				c = uuid_compare(bpnode_get_child_id(cr, r),
						 bpnode_get_child_id(cr, r+1));
				EXPECT_NE(c, 0);
			}
		} else  {
			EXPECT_EQ(uuid_compare(bpnode_get_next_id(cl),
					       bpnode_get_id(cr)), 0);
		}
	}
	
	
	
	void createNewMockSession() {
		createNewMockSession(1000, BPTREE_OPEN_OVERWRITE, 
				     BPTREE_INSERT_UNIQUE_KEY, 1);
	}
	void createNewMockSession(tapioca_bptree_id bpt_id, 
				  bptree_open_flags o_f, bptree_insert_flags i_f,
				  int n_fld)
	{
		bptree_initialize_bpt_session(bps, bpt_id, o_f, i_f);
		bptree_set_num_fields(bps, n_fld);
		for (int f = 0; f < n_fld; f++) {
			bptree_set_field_info(bps, f, sizeof(int32_t), 
					BPTREE_FIELD_COMP_INT_32, int32cmp);
		}
		
	}
	
	
};


TEST_F(BptreeCoreIntTest, DeleteElement) {
	int rv, pos, num_elem = 7;
	if(BPTREE_NODE_SIZE < num_elem) {
		printf("Cannot run test with BPTREE_NODE_SIZE %d, deg %d\n",
		       BPTREE_NODE_SIZE, BPTREE_MIN_DEGREE);
		return;
	}
	bptree_node *n = makeRandomBptreeNode(bps, num_elem);
	
	EXPECT_EQ(bpnode_size(n), num_elem);
	// Out of bounds
	delete_key_from_node(n, num_elem);
	EXPECT_EQ(bpnode_size(n), num_elem);
	EXPECT_TRUE(is_node_sane(n) == 0);
	// Border case
	delete_key_from_node(n, num_elem-1);
	EXPECT_EQ(bpnode_size(n), num_elem-1);
	EXPECT_TRUE(is_node_sane(n) == 0);
	// Middle
	delete_key_from_node(n, num_elem/2);
	EXPECT_EQ(bpnode_size(n), num_elem-2);
	EXPECT_TRUE(is_node_sane(n) == 0);
	// First
	delete_key_from_node(n, 0);
	EXPECT_EQ(bpnode_size(n), num_elem-3);
	EXPECT_TRUE(is_node_sane(n) == 0);
	
	// Final node sanity checking
	EXPECT_TRUE(is_node_ordered(bps, n) == 0);
	
}

TEST_F(BptreeCoreIntTest, DeleteFromNonTrivialTreeRandomized) {
	int *arr;
	int r, iterations= 10;
	int n = BPTREE_NODE_SIZE * (3);
	
	for (int iter = 0; iter < iterations; iter++) {
		arr = init_new_int_array(n);
		gsl_ran_shuffle(rng, arr, n, sizeof(int));
		for(int i= 0; i < n; i++) {
			r = arr[i];
			k = r*100;
			v = r*10000;
			rv = bptree_insert(bps, &k, sizeof(k), &v, sizeof(v));
			EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
		}
		//uuid_clear(nn);
		//bptree_debug(bps, BPTREE_DEBUG_DUMP_NODE_DETAILS, nn);
		uuid_clear(nn);
		rv = bptree_debug(bps, BPTREE_DEBUG_VERIFY_RECURSIVELY, nn);
		EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
		
		free(arr);
		arr = init_new_int_array(n);
		gsl_ran_shuffle(rng, arr, n, sizeof(int));
		
		for(int i= 0; i < n; i++) {
			r = arr[i];
			k = r*100;
			v = r*10000;
			rv = bptree_delete(bps, &k, sizeof(k), &v, sizeof(v)); 
			EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
			
			uuid_clear(nn);
			rv = bptree_debug(bps, BPTREE_DEBUG_VERIFY_RECURSIVELY, nn);
			EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
		}
		rv = bptree_index_first(bps, &k, &ksize, &v, &vsize); 
		EXPECT_EQ(rv, BPTREE_OP_EOF);
		free(arr);
	}
	
}
TEST_F(BptreeCoreIntTest, DeleteFromNonTrivialTree) {
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

/*@ A test case to mimic the situation when we do a delete in MySQL based on
 * a full-table scan */
TEST_F(BptreeCoreIntTest, DeleteOnConditionAndScan) {
	vector<int> deleted(0), selected(0), inserted(0), post_del_select(0); 
	int n = BPTREE_NODE_SIZE * 3;
	int rv;
	for(int i= 1; i <= n; i++) {
		k = i;
		v = i*10000;
		rv = bptree_insert(bps, &k, sizeof(k), &v, sizeof(v));
		inserted.push_back(k);
		EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	}
	printf("\n");
	
	int k2, v2, k2_sz, v2_sz;
	rv = bptree_index_first(bps, &k2, &k2_sz, &v2, &v2_sz);
	selected.push_back(k2);
	
	uuid_clear(nn);
	bptree_debug(bps, BPTREE_DEBUG_DUMP_NODE_DETAILS, nn);
	
	for (int i : inserted) 
	{
		k = i;
		v = i*10000;
		// Delete three quarters of the keys
		if (i > 9) {
			deleted.push_back(k);
			rv = bptree_delete(bps, &k, sizeof(k), &v, sizeof(v)); 
		}
		rv = bptree_index_next(bps, &k2, &k2_sz, &v2, &v2_sz);
		if (rv == BPTREE_OP_EOF) break;
		selected.push_back(k2);
		i++;
	} 
	
	rv = bptree_index_first(bps, &k2, &k2_sz, &v2, &v2_sz);
	while(rv != BPTREE_OP_EOF)
	{
		post_del_select.push_back(k2);
		rv = bptree_index_next(bps, &k2, &k2_sz, &v2, &v2_sz);
	}
	
	printf("Inserted: ");
	for (int i : inserted ) 
	{
		printf("%d ", i);
	}
	printf("\nSelected: ");
	for (int s : selected ) 
	{
		printf("%d ", s);
	}
	printf("\nDeleted: ");
	for (int d : deleted ) 
	{
		printf("%d ", d);
	}
	printf("\nPost-del select: ");
	for (int s : post_del_select ) 
	{
		printf("%d ", s);
	}
	printf("\n");
	
	EXPECT_EQ(inserted.size(), n);
	EXPECT_EQ(selected.size(), n);
	EXPECT_EQ(deleted.size() + post_del_select.size(), n);
	
	std::sort(deleted.begin(), deleted.end());
	std::sort(post_del_select.begin(), post_del_select.end());
	
	set<int> elem;
	
	set_intersection(deleted.begin(), deleted.end(),
			 post_del_select.begin(), post_del_select.end(),
			 inserter(elem, elem.end()));
	
	EXPECT_EQ(elem.size(), 0);
	
	
}

TEST_F(BptreeCoreIntTest, DeleteFromNonTrivialTreeWithDupesForward) {
	
	bps = mockBptreeSessionCreate();
	createNewMockSession(1000,
			     BPTREE_OPEN_OVERWRITE,
			     BPTREE_INSERT_ALLOW_DUPES, 1);
	
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

TEST_F(BptreeCoreIntTest, DeleteFromNonTrivialTreeWithDupesReverse) {
	
	bps = mockBptreeSessionCreate();
	createNewMockSession(1000,
			     BPTREE_OPEN_OVERWRITE,
			     BPTREE_INSERT_ALLOW_DUPES, 1);
	
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

TEST_F(BptreeCoreIntTest, DeleteFromTrivialTree) {
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

TEST_F(BptreeCoreIntTest, ConcatUnderflowLeaf) {
	
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
	EXPECT_EQ(bpnode_size(p), 0);
	EXPECT_EQ(bpnode_size(cr), 0);
	EXPECT_TRUE(!bpnode_is_node_active(p));
	EXPECT_TRUE(!bpnode_is_node_active(cr));
	EXPECT_EQ(is_node_ordered(bps,cl), 0);
	// Since this is a leaf, the split key was already there
	EXPECT_EQ(bpnode_size(cl), BPTREE_NODE_MIN_SIZE * 2 - 1);
}

TEST_F(BptreeCoreIntTest, ConcatUnderflowNonLeaf) {
	
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
	EXPECT_EQ(bpnode_size(p), 0);
	EXPECT_EQ(bpnode_size(cr), 0);
	
	EXPECT_TRUE(!bpnode_is_node_active(p)); 
	EXPECT_TRUE(!bpnode_is_node_active(cr)); 
	EXPECT_EQ(is_node_ordered(bps,cl), 0);
	EXPECT_EQ(bpnode_size(cl), BPTREE_NODE_MIN_SIZE * 2);
	
}
	
TEST_F(BptreeCoreIntTest, SplitOverflowNonLeaf) {
	
	bptree_node *cl, *cr1, *cr2, *p;

	createMiniTree(&p, &cl, BPTREE_NODE_SIZE, 
			      &cr2, BPTREE_NODE_MIN_SIZE, true);
	
	// Make a copy of all children of the full node, and make sure
	// they were distributed properly across the two nodes
	uuid_t children[BPTREE_NODE_SIZE+1];
	for (int i=0; i<= BPTREE_NODE_SIZE; i++) {
		uuid_copy(children[i], bpnode_get_child_id(cl, i)); 
	}
	
	cr1 = bpnode_new();
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
		EXPECT_EQ(uuid_compare(bpnode_get_child_id(cl, i), children[i]), 0);
		EXPECT_EQ(uuid_compare(bpnode_get_child_id(cr1, i),
				       children[i+BPTREE_NODE_MIN_SIZE+1]), 0);
	}
	
	EXPECT_EQ(bpnode_size(p), 2);
	EXPECT_EQ(bpnode_size(cl), BPTREE_NODE_MIN_SIZE);
	EXPECT_EQ(bpnode_size(cr1), BPTREE_NODE_MIN_SIZE);
	EXPECT_EQ(bpnode_size(cr2), BPTREE_NODE_MIN_SIZE);
	
	EXPECT_EQ(are_split_cells_valid(bps, p, 0, cl, cr1), 0);
	
	// Ensure links were maintained properly
	EXPECT_EQ(uuid_compare(bpnode_get_child_id(p,0), bpnode_get_id(cl)), 0);
	EXPECT_EQ(uuid_compare(bpnode_get_child_id(p,1), bpnode_get_id(cr1)), 0);
	EXPECT_EQ(uuid_compare(bpnode_get_child_id(p,2), bpnode_get_id(cr2)), 0);
}

TEST_F(BptreeCoreIntTest, SplitOverflowLeaf) {
	bptree_node *cl, *cr1, *cr2, *p;

	createMiniTree(&p, &cl, BPTREE_NODE_SIZE, 
			      &cr2, BPTREE_NODE_MIN_SIZE, false);
	
	cr1 = bpnode_new();
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
	
	EXPECT_TRUE(bpnode_size(p) == 2);
	EXPECT_EQ(bpnode_size(cl), BPTREE_NODE_MIN_SIZE);
	EXPECT_EQ(bpnode_size(cr1), BPTREE_NODE_MIN_SIZE + 1);
	EXPECT_EQ(bpnode_size(cr2), BPTREE_NODE_MIN_SIZE);
	
	EXPECT_EQ(are_split_cells_valid(bps, p, 0, cl, cr1), 0);
	
	// Ensure links were maintained properly
	EXPECT_EQ(uuid_compare(bpnode_get_child_id(p,0), bpnode_get_id(cl)), 0);
	EXPECT_EQ(uuid_compare(bpnode_get_child_id(p,1), bpnode_get_id(cr1)), 0);
	EXPECT_EQ(uuid_compare(bpnode_get_child_id(p,2), bpnode_get_id(cr2)), 0);
	
	
	
	EXPECT_EQ(uuid_compare(bpnode_get_next_id(cl), bpnode_get_id(cr1)), 0);
	EXPECT_EQ(uuid_compare(bpnode_get_next_id(cr1), bpnode_get_id(cr2)), 0);
	
}

TEST_F(BptreeCoreIntTest, RedistUnderflowNonLeaf) {
	bptree_node *cl, *cr, *p;

	///////////////////////////////////////////////////////////////
	// Right-to-left transfer 
	createMiniTree(&p, &cl, BPTREE_NODE_MIN_SIZE - 1, 
			      &cr, BPTREE_NODE_MIN_SIZE + 1, true);
	
	redistribute_keys(p,cl,cr,0);
	
	EXPECT_TRUE(bpnode_size(p) == 1);
	EXPECT_TRUE(bpnode_size(cl) == BPTREE_NODE_MIN_SIZE);
	EXPECT_TRUE(bpnode_size(cr) == BPTREE_NODE_MIN_SIZE);
		
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
	
	EXPECT_TRUE(bpnode_size(p) == 1);
	EXPECT_TRUE(bpnode_size(cl) == BPTREE_NODE_MIN_SIZE);
	EXPECT_TRUE(bpnode_size(cr) == BPTREE_NODE_MIN_SIZE);
		
		
	miniTreeSanityChecks(p,cl,cr);
}
	
TEST_F(BptreeCoreIntTest, RedistUnderflowLeaf) {
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
	
	EXPECT_TRUE(bpnode_size(p) == 1);
	EXPECT_TRUE(bpnode_size(cl) == BPTREE_NODE_MIN_SIZE);
	EXPECT_TRUE(bpnode_size(cr) == BPTREE_NODE_MIN_SIZE);
		
	miniTreeSanityChecks(p,cl,cr);
		
	///////////////////////////////////////////////////////////////
	// Left-to-right transfer 
	createMiniTree(&p, &cl, BPTREE_NODE_MIN_SIZE + 1, 
			      &cr, BPTREE_NODE_MIN_SIZE - 1, false);
	
	redistribute_keys(p,cl,cr,0);
	
	EXPECT_TRUE(bpnode_size(p) == 1);
	EXPECT_TRUE(bpnode_size(cl) == BPTREE_NODE_MIN_SIZE);
	EXPECT_TRUE(bpnode_size(cr) == BPTREE_NODE_MIN_SIZE);
		
	miniTreeSanityChecks(p,cl,cr);
}
	
TEST_F(BptreeCoreIntTest, InsertToFull) {
	// Inserting more than nodesize ensures that at least one split happens
	int n = BPTREE_NODE_SIZE;
	int *arr = init_new_int_array(n);
	gsl_ran_shuffle(rng, arr, n, sizeof(int));
	for(int i= 0; i < n; i++) {
		int r = arr[i];
		k = r*100;
		v = r*10000;
		rv = bptree_insert(bps, &k, sizeof(k), &v, sizeof(v));
		EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	}
	printf("\n");
	
	gsl_ran_shuffle(rng, arr, n, sizeof(int));
	for(int i= 0; i < n; i++) {
		int r = arr[i];
		k = r*100;
		rv = bptree_search(bps, &k, sizeof(k), &v, &vsize); 
		EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
		EXPECT_EQ(vsize, sizeof(v));
		EXPECT_EQ(v, r*10000);
	}
}

TEST_F(BptreeCoreIntTest, MoreThanOneNodeInsert) {
	// Inserting more than nodesize ensures that at least one split happens
	int n = 5 * BPTREE_NODE_SIZE;
	int *arr = init_new_int_array(n);
	gsl_ran_shuffle(rng, arr, n, sizeof(int));
	for(int i= 0; i < n; i++) {
		int r = arr[i];
		k = r*100;
		v = r*10000;
		rv = bptree_insert(bps, &k, sizeof(k), &v, sizeof(v));
		EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	}
	
	gsl_ran_shuffle(rng, arr, n, sizeof(int));
	for(int i= 0; i < n; i++) {
		int r = arr[i];
		k = r*100;
		rv = bptree_search(bps, &k, sizeof(k), &v, &vsize); 
		EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
		EXPECT_EQ(vsize, sizeof(v));
		EXPECT_EQ(v, r*10000);
	}
}

TEST_F(BptreeCoreIntTest, InsertUpdateSearch) {
	int n = 5 * BPTREE_NODE_SIZE;
	int *arr = init_new_int_array(n);
	gsl_ran_shuffle(rng, arr, n, sizeof(int));
	for(int i= 0; i < n; i++) {
		int r = arr[i];
		k = r*100;
		v = r*10000;
		rv = bptree_insert(bps, &k, sizeof(k), &v, sizeof(v));
		EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	}
	
	gsl_ran_shuffle(rng, arr, n, sizeof(int));
	for(int i= 0; i < n; i++) {
		int r = arr[i];
		k = r*100;
		v = r*30000;
		rv = bptree_update(bps, &k, sizeof(k), &v, sizeof(v)); 
		EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	}
	uuid_clear(nn);
	rv = bptree_debug(bps, BPTREE_DEBUG_VERIFY_RECURSIVELY, nn);
	EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	
	gsl_ran_shuffle(rng, arr, n, sizeof(int));
	for(int i= 0; i < n; i++) {
		int r = arr[i];
		k = r*100;
		rv = bptree_search(bps, &k, sizeof(k), &v, &vsize); 
		EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
		EXPECT_EQ(vsize, sizeof(v));
		EXPECT_EQ(v, r*30000);
	}
	uuid_clear(nn);
	rv = bptree_debug(bps, BPTREE_DEBUG_VERIFY_RECURSIVELY, nn);
	EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
}

TEST_F(BptreeCoreIntTest, InsertDupe) {
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

TEST_F(BptreeCoreIntTest, CursorEmptyTable) {
	rv = bptree_search(bps, &k, sizeof(k), &v, &vsize);
	EXPECT_EQ(rv, BPTREE_OP_KEY_NOT_FOUND);
	EXPECT_EQ(vsize, 0);
	
	rv = bptree_index_first(bps, &k, &ksize, &v, &vsize);
	EXPECT_EQ(rv, BPTREE_OP_EOF);
	EXPECT_EQ(ksize, 0);
	EXPECT_EQ(vsize, 0);
	rv = bptree_index_next(bps, &k, &ksize, &v, &vsize);
	EXPECT_EQ(rv, BPTREE_OP_EOF);
	EXPECT_EQ(ksize, 0);
	EXPECT_EQ(vsize, 0);
}

TEST_F(BptreeCoreIntTest, CursorSingleRowTable) {
	k = 1000;
	v = 1234;
	
	rv = bptree_insert(bps, &k, sizeof(k), &v, sizeof(v));
	EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	
	rv = bptree_index_first(bps, &k, &ksize, &v, &vsize);
	EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
	EXPECT_EQ(ksize, sizeof(k));
	EXPECT_EQ(vsize, sizeof(v));
	
	rv = bptree_index_next(bps, &k, &ksize, &v, &vsize);
	EXPECT_EQ(rv, BPTREE_OP_EOF);
	EXPECT_EQ(ksize, 0);
	EXPECT_EQ(vsize, 0);
}

TEST_F(BptreeCoreIntTest, CursorUnset) {
	k = 1000;
	v = 1234;
	
	rv = bptree_insert(bps, &k, sizeof(k), &v, sizeof(v));
	EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	
	k++;
	rv = bptree_insert(bps, &k, sizeof(k), &v, sizeof(v));
	EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	
	rv = bptree_index_next(bps, &k, &ksize, &v, &vsize);
	EXPECT_EQ(rv, BPTREE_OP_CURSOR_NOT_SET);
	EXPECT_EQ(ksize, 0);
	EXPECT_EQ(vsize, 0);
}

TEST_F(BptreeCoreIntTest, CursorFullTraversal) {
	int n = 5 * BPTREE_NODE_SIZE;
	int *arr = init_new_int_array(n);
	gsl_ran_shuffle(rng, arr, n, sizeof(int));
	for(int i= 0; i < n; i++) {
		int r = arr[i];
		k = r*100;
		v = r*10000;
		rv = bptree_insert(bps, &k, sizeof(k), &v, sizeof(v));
		EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	}
	int cnt = 1;
	uuid_clear(nn);
	bptree_debug(bps, BPTREE_DEBUG_DUMP_NODE_DETAILS, nn);
	rv = bptree_index_first(bps, &k, &ksize, &v, &vsize);
	EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
	EXPECT_EQ(k, 0);
	EXPECT_EQ(v, 0);
	EXPECT_EQ(ksize, sizeof(k));
	EXPECT_EQ(vsize, sizeof(v));
	
	for (int i=1;i < n; i++) {
		rv = bptree_index_next(bps, &k, &ksize, &v, &vsize);
		EXPECT_EQ(k, i*100);
		EXPECT_EQ(v, i*10000);
		EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
	}
	
	rv = bptree_index_next(bps, &k, &ksize, &v, &vsize);
	EXPECT_EQ(ksize, 0);
	EXPECT_EQ(vsize, 0);
	EXPECT_EQ(rv, BPTREE_OP_EOF);
	
}	

TEST_F(BptreeCoreIntTest, CursorPartialKey) {
	int n = 3 * BPTREE_NODE_SIZE;
	int n_2 = 3; // number of second-level keys
	unsigned char kbuf[sizeof(int)*2];
	bps = mockBptreeSessionCreate();
	createNewMockSession(1000,
			     BPTREE_OPEN_OVERWRITE,
			     BPTREE_INSERT_UNIQUE_KEY, 2);
	
	int *arr = init_new_int_array(n);
	gsl_ran_shuffle(rng, arr, n, sizeof(int));
	for(int i= 0; i < n; i++) {
		int r = arr[i];
		k = r*100;
		v = r*10000;
		for (int j = 0; j < n_2; j++) {
			memcpy(kbuf, &k, sizeof(int));
			memcpy(kbuf+sizeof(int), &j, sizeof(int));
			rv = bptree_insert(bps, kbuf, sizeof(int)*2, &v, sizeof(v));
			EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
		}
	}
	uuid_clear(nn);
	bptree_debug(bps, BPTREE_DEBUG_DUMP_NODE_DETAILS, nn);
	gsl_ran_shuffle(rng, arr, n, sizeof(int));
	for(int i= 0; i < n; i++) {
		int r = arr[i];
		k = r*100;
		rv = bptree_search(bps, &k, sizeof(k), &v, &vsize); 
//		EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
	//	EXPECT_EQ(vsize, sizeof(v));
		//EXPECT_EQ(v, r*10000);
		
		for (int j = 0; j < n_2; j++) {
			rv = bptree_index_next(bps, kbuf, &ksize, &v, &vsize);
			EXPECT_EQ(ksize, sizeof(k)*2);
			EXPECT_EQ(vsize, sizeof(v));
			EXPECT_EQ(*(int *)kbuf, r*100);
			EXPECT_EQ(*(int *)(kbuf+sizeof(int)), j);
			EXPECT_EQ(v, r*10000);
			EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
		}
	}
	
	rv = bptree_index_next(bps, &k, &ksize, &v, &vsize);
	//EXPECT_EQ(ksize, 0);
	//EXPECT_EQ(vsize, 0);
	//EXPECT_EQ(rv, BPTREE_OP_EOF);
	
}	