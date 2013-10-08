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

#include "bptree_unittest.h"

class BptreeInterfaceTest : public BptreeTestBase {
};


TEST_F(BptreeInterfaceTest, TestEmptyBptree)
{
	rv = tapioca_bptree_index_first(th, tbpt_id, k, &ksize, v, &vsize);

	EXPECT_EQ(rv, BPTREE_OP_EOF);
}

TEST_F(BptreeInterfaceTest, TestUpdate)
{
	// Randomly sample from an array of size keys and insert into our btree
	int i, j, n, r;
	tapioca_bptree_set_num_fields(th, tbpt_id, 1);
	tapioca_bptree_set_field_info(th, tbpt_id, 0, 5, BPTREE_FIELD_COMP_STRNCMP);

	char v2[10] = "abcd12345";
	for (i = 1; i <= keys; i++)
	{
		sprintf(k,"%03d",i);
		rv = tapioca_bptree_insert(th, tbpt_id, k, 5, v, 10, BPTREE_INSERT_UNIQUE_KEY);
		ASSERT_EQ(rv, BPTREE_OP_SUCCESS);
		rv = tapioca_bptree_update(th, tbpt_id, k, 5, v2, 10);
		ASSERT_EQ(rv, BPTREE_OP_SUCCESS);
		rv = tapioca_commit(th);
		EXPECT_GE(0, rv);
		if (i % 250 == 0) printf("Updated %d keys\n", i);
	}
// TODO Add ordering check
/*	printf("Verifying ordering...OFF ");
	rv1 = verify_tapioca_bptree_order(th, tbpt_id, BPTREE_VERIFY_RECURSIVELY);
	//rv2 = verify_tapioca_bptree_order(th, tbpt_id, BPTREE_VERIFY_SEQUENTIALLY);
	dump_tapioca_bptree_contents(th, tbpt_id, 1, 0);
	return (rv1 && rv2);
	*/
}

TEST_F(BptreeInterfaceTest, MultiFieldInsertDupe) 
{
	tapioca_bptree_set_num_fields(th, tbpt_id, 4);
	tapioca_bptree_set_field_info(th, tbpt_id, 0, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	tapioca_bptree_set_field_info(th, tbpt_id, 1, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	tapioca_bptree_set_field_info(th, tbpt_id, 2, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	tapioca_bptree_set_field_info(th, tbpt_id, 3, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	int a, b, c, d;
	a = b = c = d = 1;
	memcpy(k, &a, 4);
	memcpy(k + 4, &b, 4);
	memcpy(k + 8, &c, 4);
	memcpy(k + 12, &d, 4);
	rv = tapioca_bptree_insert(th, tbpt_id, k, 16, v, 4, BPTREE_INSERT_UNIQUE_KEY);
	EXPECT_EQ(BPTREE_OP_SUCCESS, rv);
	d = 2;
	memcpy(k + 12, &d, 4);
	rv = tapioca_bptree_insert(th, tbpt_id, k, 16, v, 4, BPTREE_INSERT_UNIQUE_KEY);
	EXPECT_EQ(BPTREE_OP_SUCCESS, rv);
	d = 1;
	memcpy(k + 12, &d, 4);
	rv = tapioca_bptree_insert(th, tbpt_id, k, 16, v, 4, BPTREE_INSERT_UNIQUE_KEY);
	EXPECT_EQ(BPTREE_ERR_DUPLICATE_KEY_INSERTED, rv);
}

TEST_F(BptreeInterfaceTest, MultiFieldInsertUpdate) 
{
	char *kptr;
	tapioca_bptree_set_num_fields(th, tbpt_id, 4);
	tapioca_bptree_set_field_info(th, tbpt_id, 0, 5, BPTREE_FIELD_COMP_STRNCMP);
	tapioca_bptree_set_field_info(th, tbpt_id, 1, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	tapioca_bptree_set_field_info(th, tbpt_id, 2, 5, BPTREE_FIELD_COMP_STRNCMP);
	tapioca_bptree_set_field_info(th, tbpt_id, 3, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);

	for (int i = 1; i <= keys; i++)
	{
		sprintf(k, "a%03d", i);
		kptr = k;
		memcpy(kptr + 5, &i, sizeof(int32_t));
		memcpy(kptr + 9, k, 5);
		memcpy(kptr + 14, &i, sizeof(int32_t));

		rv = tapioca_bptree_insert(th, tbpt_id, k, 18, v, 5, BPTREE_INSERT_UNIQUE_KEY);
		EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
		rv = tapioca_commit(th);
		EXPECT_GE(0, rv);
		if (i % 250 == 0) printf("Inserted %d keys\n", i);
	}

	char v2[10] = "abcd12345";
	for (int i = 1; i <= keys; i++)
	{
		sprintf(k, "a%03d", i);
		kptr = k;
		memcpy(kptr + 5, &i, sizeof(int32_t));
		memcpy(kptr + 9, k, 5);
		memcpy(kptr + 14, &i, sizeof(int32_t));

		rv = tapioca_bptree_update(th, tbpt_id, k, 18, v2, 10);
		ASSERT_EQ(rv, BPTREE_OP_SUCCESS);
		rv = tapioca_commit(th);
		ASSERT_GE(0, rv);
		if (i % 250 == 0) printf("Updated %d keys\n", i);
	}

	//printf("Verifying ordering... OFF");
//	rv1 = verify_tapioca_bptree_order(th, tbpt_id, BPTREE_VERIFY_RECURSIVELY);
//	rv2 = verify_tapioca_bptree_order(th, tbpt_id, BPTREE_VERIFY_SEQUENTIALLY);
//	dump_tapioca_bptree_contents(th, tbpt_id, 1, 1);
}

class BptreeCursorTest : public BptreeTestBase {
	
protected:
	bool DBUG;
	void SetUp() {
		BptreeTestBase::SetUp();
		//num_threads = 1;
		keys = 200;
		DBUG = true;
	}
	
};

TEST_F(BptreeCursorTest, TestSingleElementTree)
{
	char kk[5] = "aaaa";
	char vv[5] = "cccc";
	vsize = 5;

	tapioca_bptree_set_num_fields(th,tbpt_id, 1);
	tapioca_bptree_set_field_info(th,tbpt_id, 0, 5, BPTREE_FIELD_COMP_STRNCMP);

	rv = tapioca_bptree_insert(th, tbpt_id, &kk, 5, &vv, 5, BPTREE_INSERT_UNIQUE_KEY);
	EXPECT_EQ(rv, BPTREE_OP_SUCCESS);

	rv = tapioca_commit(th);
	EXPECT_TRUE(rv >= 0);

	rv = tapioca_bptree_index_first(th, tbpt_id, k, &ksize, v, &vsize);
	EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
	rv = tapioca_bptree_index_next(th, tbpt_id, k, &ksize, v, &vsize);
	EXPECT_EQ(rv, BPTREE_OP_EOF);
	// Did tapioca_bptree_first fetch the correct values?
	EXPECT_EQ(0, memcmp(k, kk, 5));
	EXPECT_EQ(0, memcmp(v, vv, 5));
}



TEST_F(BptreeCursorTest, TestTwoElementBptree) 
{
	char k1[5] = "aaaa";
	char k2[5] = "bbbb";
	char vv[5] = "cccc";
	vsize = 5;

	tapioca_bptree_set_num_fields(th,tbpt_id, 1);
	tapioca_bptree_set_field_info(th,tbpt_id, 0, 5, BPTREE_FIELD_COMP_STRNCMP);

	rv = tapioca_bptree_insert(th, tbpt_id, k2, 5, vv, 5, BPTREE_INSERT_UNIQUE_KEY);
	EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	rv = tapioca_bptree_insert(th, tbpt_id, k1, 5, vv, 5, BPTREE_INSERT_UNIQUE_KEY);
	EXPECT_EQ(rv, BPTREE_OP_SUCCESS);

	rv = tapioca_commit(th);
	EXPECT_TRUE(rv >= 0);

	rv = tapioca_bptree_index_first(th, tbpt_id, k, &ksize, v, &vsize);
	EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
	EXPECT_EQ(0, memcmp(k1, k, 5));
	EXPECT_EQ(0, memcmp(v, vv, 5));
	rv = tapioca_bptree_index_next(th, tbpt_id, k, &ksize, v, &vsize);
	EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
	EXPECT_EQ(0, memcmp(k2, k, 5));
	EXPECT_EQ(0, memcmp(v, vv, 5));
	rv = tapioca_bptree_index_next(th, tbpt_id, k, &ksize, v, &vsize);
	EXPECT_EQ(rv, BPTREE_OP_EOF);

}



TEST_F(BptreeCursorTest, TestPartialKeyTraversal) 
{
	int n,key,r;
	int64_t k1;
	int32_t k2;

	tapioca_bptree_set_num_fields(th,tbpt_id, 2);
	tapioca_bptree_set_field_info(th,tbpt_id, 0, sizeof(int64_t), 
								  BPTREE_FIELD_COMP_INT_64);
	tapioca_bptree_set_field_info(th,tbpt_id, 1, sizeof(int32_t), 
								  BPTREE_FIELD_COMP_INT_32);

	strcpy(v, "cccc");
	int* arr;
	arr = (int *) malloc(keys * sizeof(int));
	for (int i = 0; i < keys; i++) arr[i] = i;
	n = keys;
	int total_retries = 0;
	for (int i = 0; i < keys / 10; i++)
	{
		for (int j = 0; j < 5; j++) {
			r = rand() % (n);
			key = arr[r];
			arr[r] = arr[n-1];
			n--;
			k1 = key;
			k2 = j;
			memcpy(k, &k1, sizeof(int64_t));
			memcpy(k+sizeof(int64_t), &k2, sizeof(int32_t));
			rv = tapioca_bptree_insert(th, tbpt_id, k, 
					sizeof(int64_t) + sizeof(int32_t), v, 4,
					BPTREE_INSERT_UNIQUE_KEY);
			rv = tapioca_commit(th);
			EXPECT_GE(rv, 0);
		}
	}

	if (DBUG) {
		tapioca_bptree_debug(th,tbpt_id, BPTREE_DEBUG_DUMP_GRAPHVIZ);
	}
	return;
	
	// Now we have a tree with some data; let's do some partial key searching
	for (int i = 1; i < keys/10; i++)
	{
		k1 = i;
		memcpy(k, &k1, sizeof(int64_t));
		
		rv = tapioca_bptree_search(th, tbpt_id, k, sizeof(int64_t), v, &vsize);
		EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
		EXPECT_EQ(strcmp(v, "cccc"), 0);
		
		rv = tapioca_bptree_index_next(th, tbpt_id, k, &ksize, v, &vsize);
		EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
		EXPECT_EQ(ksize,sizeof(int64_t)+sizeof(int32_t));
		EXPECT_EQ(vsize,4);
		EXPECT_EQ(strcmp(v, "cccc"), 0);
		EXPECT_EQ(*(int32_t*)(k+sizeof(int64_t)), 0);
		memcpy(k, &k2, ksize);
		
		rv = tapioca_bptree_index_next(th, tbpt_id, k, &ksize, v, &vsize);
		EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
		EXPECT_EQ(*(int32_t*)(k+sizeof(int64_t)), 1);
		EXPECT_EQ(strcmp(v, "cccc"), 0);
	}
	
	if (DBUG) {
		tapioca_bptree_debug(th,tbpt_id, BPTREE_DEBUG_DUMP_GRAPHVIZ);
	}

}
