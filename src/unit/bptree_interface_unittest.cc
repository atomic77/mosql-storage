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

int *init_new_int_array(int sz) 
{
	int *arr;  
	arr = (int *) malloc(sz * sizeof(int));
	for (int i = 0; i < sz; i++) arr[i] = i;
	return arr;
}

int sample_without_replacement(int *arr, int *n)
{
	int r,k;
	r = rand() % (*n);
	k = arr[r];
	arr[r] = arr[*n-1];
	(*n)--;
	return k;
}


class BptreeInterfaceTest : public BptreeTestBase {
	
	void SetUp() {
		BptreeTestBase::SetUp();
		//keys = 50;
	}
};


TEST_F(BptreeInterfaceTest, TestEmptyBptree)
{
	rv = tapioca_bptree_index_first(th, tbpt_id, k, &ksize, v, &vsize);

	EXPECT_EQ(rv, BPTREE_OP_EOF);
}

TEST_F(BptreeInterfaceTest, TestRandomBatchedInsert)
{
	int batch = 5, r;
	tapioca_bptree_set_num_fields(th, tbpt_id, 1);
	tapioca_bptree_set_field_info(th, tbpt_id, 0, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	int n = keys;
	int *arr = init_new_int_array(n);
	for (int i = 0; i < keys; i++)
	{
		r = sample_without_replacement(arr, &n);
		rv = tapioca_bptree_insert(th, tbpt_id, &r, 5, &batch, sizeof(int));
		ASSERT_EQ(rv, BPTREE_OP_SUCCESS);
		rv = tapioca_commit(th);
		EXPECT_GE(0, rv);
		if (i % 250 == 0) printf("Updated %d keys\n", i);
	}
}

TEST_F(BptreeInterfaceTest, TestUpdate)
{
	int i, j, n, r, sz;
	tapioca_bptree_set_num_fields(th, tbpt_id, 1);
	tapioca_bptree_set_field_info(th, tbpt_id, 0, 5, BPTREE_FIELD_COMP_STRNCMP);

	char k1[5];
	char v2[10];
	char v3[10];
	// Check for pre and post commit
	for (i = 1; i <= keys; i++)
	{
		sprintf(k1,"k%03d",i);
		rv = tapioca_bptree_insert(th, tbpt_id, k1, 5, v, 10);
		ASSERT_EQ(rv, BPTREE_OP_SUCCESS);
		memset(v2,0,10);
		memset(v3,0,10);
		sprintf(v2, "abcdef%03d", i);
		rv = tapioca_bptree_update(th, tbpt_id, k1, 5, v2, 10);
		ASSERT_EQ(rv, BPTREE_OP_SUCCESS);
		rv = tapioca_bptree_search(th, tbpt_id, k1, 5, v3, &sz);
		ASSERT_EQ(rv, BPTREE_OP_KEY_FOUND);
		ASSERT_EQ(sz,10);
		ASSERT_EQ(strncmp(v2, v3, 10), 0);
		rv = tapioca_commit(th);
		EXPECT_GE(0, rv);
		if (i % 250 == 0) printf("Updated %d keys\n", i);
	}
	
	DBUG = true;
	printf("Re-checking post-commit\n");
	for (i = 1; i <= keys; i++)
	{
		sprintf(k1,"k%03d",i);
		memset(v2,0,10);
		memset(v3,0,10);
		sprintf(v2, "abcdef%03d", i);
		rv = tapioca_bptree_search(th, tbpt_id, k1, 5, v3, &sz);
		EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
		EXPECT_EQ(sz,10);
		EXPECT_EQ(strncmp(v2, v3, 10), 0);
		tapioca_commit(th);
	}
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
	rv = tapioca_bptree_insert(th, tbpt_id, k, 16, v, 4);
	EXPECT_EQ(BPTREE_OP_SUCCESS, rv);
	d = 2;
	memcpy(k + 12, &d, 4);
	rv = tapioca_bptree_insert(th, tbpt_id, k, 16, v, 4);
	EXPECT_EQ(BPTREE_OP_SUCCESS, rv);
	d = 1;
	memcpy(k + 12, &d, 4);
	rv = tapioca_bptree_insert(th, tbpt_id, k, 16, v, 4);
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

		rv = tapioca_bptree_insert(th, tbpt_id, k, 18, v, 5);
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

}

class BptreeCursorTest : public BptreeTestBase {
	
protected:
	int pkeys, skeys;
	void SetUp() {
		BptreeTestBase::SetUp();
		//num_threads = 1;
		pkeys = 50;
		skeys = 3;
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

	rv = tapioca_bptree_insert(th, tbpt_id, &kk, 5, &vv, 5);
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

	rv = tapioca_bptree_insert(th, tbpt_id, k2, 5, vv, 5);
	EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	rv = tapioca_bptree_insert(th, tbpt_id, k1, 5, vv, 5);
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
	int* arr = init_new_int_array(pkeys);
	n = pkeys;
	int total_retries = 0;
	for (int i = 0; i < pkeys; i++)
	{
		key = sample_without_replacement(arr, &n);
		k1 = key;
		for (int j = 0; j < skeys; j++) {
			k2 = j;
			//printf("Inserting %lld,%lld \n", k1,k2);
			memcpy(k, &k1, sizeof(int64_t));
			memcpy(k+sizeof(int64_t), &k2, sizeof(int32_t));
			rv = tapioca_bptree_insert(th, tbpt_id, k, 
					sizeof(int64_t) + sizeof(int32_t), v, 4 );
			rv = tapioca_commit(th);
			EXPECT_GE(rv, 0);
		}
	}

	if (DBUG) {
		tapioca_bptree_debug(th,tbpt_id, BPTREE_DEBUG_DUMP_GRAPHVIZ);
	}
	free(arr);
	arr = init_new_int_array(pkeys); // to avoid an EOF
	n = pkeys;
	
	// Now we have a tree with some data; let's do some partial key searching
	for (int i = 0; i < pkeys; i++)
	{
		memset(k, 0, 12);
		memset(v, 0, 12);
		k1 = sample_without_replacement(arr, &n);
		memcpy(k, &k1, sizeof(int64_t));
		
		if (k1 == 4123122) {
				printf("ready...");
		}
		rv = tapioca_bptree_search(th, tbpt_id, k, sizeof(int64_t), v, &vsize);
		
		rv = tapioca_bptree_index_next(th, tbpt_id, k, &ksize, v, &vsize);
		EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
		EXPECT_EQ(ksize,sizeof(int64_t)+sizeof(int32_t));
		EXPECT_EQ(vsize,4);
		EXPECT_EQ(strcmp(v, "cccc"), 0);
		EXPECT_EQ(*(int32_t*)(k+sizeof(int64_t)), 0);

		
		rv = tapioca_bptree_index_next(th, tbpt_id, k, &ksize, v, &vsize);
		EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
		EXPECT_EQ(*(int32_t*)(k+sizeof(int64_t)), 1);
		EXPECT_EQ(strcmp(v, "cccc"), 0);
	}
	
	if (DBUG) {
		tapioca_bptree_debug(th,tbpt_id, BPTREE_DEBUG_DUMP_GRAPHVIZ);
		tapioca_bptree_debug(th,tbpt_id, BPTREE_DEBUG_DUMP_RECURSIVELY);
		tapioca_bptree_debug(th,tbpt_id, BPTREE_DEBUG_DUMP_SEQUENTIALLY);
		tapioca_bptree_debug(th,tbpt_id, BPTREE_DEBUG_VERIFY_RECURSIVELY);
		tapioca_bptree_debug(th,tbpt_id, BPTREE_DEBUG_VERIFY_SEQUENTIALLY);
		tapioca_bptree_debug(th,tbpt_id, BPTREE_DEBUG_INDEX_RECURSIVE_SCAN);
	}

}
