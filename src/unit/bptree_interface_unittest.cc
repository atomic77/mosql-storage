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
//#include "bptree_test_base.h"
//#include "tapioca.h"
//#include "tapioca_btree.h"
//#include "remote_mock.h"
//#include "test_helpers.h"

#include "gtest.h"
#define LARGE_BUFFER_SZ 128000

extern "C" {
	#include "bplustree.h"
}


class BptreeCursorTest : public testing::Test {

protected:
    bptree_session *bps;
	tapioca_bptree_id tbpt_id;
	enum bptree_open_flags open_flags;
	
	int rv, rv1, rv2;
	char k[LARGE_BUFFER_SZ];
	char v[LARGE_BUFFER_SZ];
	int32_t ksize, vsize;

	virtual void SetUp() {
		//system("cd ..; ./start.sh > /dev/null; cd unit");
		memset(k, 0,LARGE_BUFFER_SZ);
		memset(v, 0,LARGE_BUFFER_SZ);
		int rv;
		tbpt_id = 123;
		open_flags = BPTREE_OPEN_OVERWRITE;
		rv = bptree_initialize_bpt_session_no_commit(bps, tbpt_id, open_flags, 321);
        EXPECT_EQ(rv, BPTREE_OP_SUCCESS);
	}
	
	virtual void TearDown() {
		// blah
	}

};


TEST_F(BptreeCursorTest, TestEmptyBptree)
{
	rv = bptree_index_first(tbpt_id, k, &ksize, v, &vsize);

	EXPECT_EQ(rv, BPTREE_OP_EOF);
}

class BptreeInterfaceTest : public testing::Test {

protected:
    tapioca_handle* th;
	
	virtual void SetUp() {
		//system("cd ..; ./start.sh > /dev/null; cd unit");
		system("cd ..; bash scripts/launch_all.sh --kill-all --clear-db > /dev/null; cd -");
		sleep(1);
        th = tapioca_open("127.0.0.1", 5555);
        EXPECT_NE(th, (tapioca_handle*)NULL);
	}
	
	virtual void TearDown() {
        tapioca_close(th);
		//system("cd ..; ./stop.sh; rm *.log; sleep 2; rm -rf /tmp/[pr]log_*; cd unit");
		system("killall -q cm tapioca example_acceptor example_proposer rec");
		sleep(2);
		system("killall -q -9 cm tapioca example_acceptor example_proposer rec");
	}

};

/*
int TEST_F(BptreeInterfaceTest, TestUpdate)
{
	// Randomly sample from an array of size keys and insert into our btree
	int i, j, n, r, rv, rv1, rv2, keys;
	char k[5] = "a000";
	char v[5] = "v000";
	tapioca_handle *th;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;
	tapioca_bptree_id tbpt_id;
	th = tapioca_open("127.0.0.1", 5555);
	tbpt_id = tapioca_bptree_initialize_bpt_session(th, 100, BPTREE_OPEN_OVERWRITE);
	tapioca_bptree_set_num_fields(th, tbpt_id, 1);
	tapioca_bptree_set_field_info(th, tbpt_id, 0, 5, BPTREE_FIELD_COMP_STRNCMP);

	for (i = 1; i <= keys; i++)
	{
		k[1] = 0x30 + ((i / 100) % 10);
		k[2] = 0x30 + ((i / 10) % 10);
		k[3] = 0x30 + (i % 10);
		rv = tapioca_bptree_insert(th, tbpt_id, &k, 5, &v, 5, BPTREE_INSERT_UNIQUE_KEY);
		if (rv == BPTREE_ERR_DUPLICATE_KEY_INSERTED)
		{
			printf("Tried to insert duplicate key %s\n", k);
		}
		tapioca_commit(th);
		if (i % 250 == 0)
			printf("Inserted %d keys\n", i);
	}

	char v2[10] = "abcd12345";
	for (i = 1; i <= keys / 2; i++)
	{
		k[1] = 0x30 + ((i / 100) % 10);
		k[2] = 0x30 + ((i / 10) % 10);
		k[3] = 0x30 + (i % 10);
		tapioca_bptree_update(th, tbpt_id, k, 5, v2, 10);
		tapioca_commit(th);
		if (i % 250 == 0)
			printf("Updated %d keys\n", i);
	}

	printf("Verifying ordering...OFF ");
	rv1 = verify_tapioca_bptree_order(th, tbpt_id, BPTREE_VERIFY_RECURSIVELY);
	//rv2 = verify_tapioca_bptree_order(th, tbpt_id, BPTREE_VERIFY_SEQUENTIALLY);
	dump_tapioca_bptree_contents(th, tbpt_id, 1, 0);
	return (rv1 && rv2);
}

int TEST_F(BptreeInterfaceTest, MultiFieldInsertDupe) 
//test_multi_field_insert_dupe(int keys)
{
	tapioca_handle *th;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;
	tapioca_bptree_id tbpt_id;
	th = tapioca_open("127.0.0.1", 5555);
	tbpt_id = tapioca_bptree_initialize_bpt_session(th, 100, BPTREE_OPEN_OVERWRITE);
	tapioca_bptree_set_num_fields(th, tbpt_id, 4);
	tapioca_bptree_set_field_info(th, tbpt_id, 0, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	tapioca_bptree_set_field_info(th, tbpt_id, 1, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	tapioca_bptree_set_field_info(th, tbpt_id, 2, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	tapioca_bptree_set_field_info(th, tbpt_id, 3, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	int a, b, c, d;
	a = b = c = d = 1;
	unsigned char k[16];
	unsigned char v[4];
	memcpy(k, &a, 4);
	memcpy(k + 4, &b, 4);
	memcpy(k + 8, &c, 4);
	memcpy(k + 12, &d, 4);
	tapioca_bptree_insert(th, tbpt_id, k, 16, v, 4, BPTREE_INSERT_UNIQUE_KEY);
	d = 2;
	memcpy(k + 12, &d, 4);
	tapioca_bptree_insert(th, tbpt_id, k, 16, v, 4, BPTREE_INSERT_UNIQUE_KEY);
	d = 1;
	memcpy(k + 12, &d, 4);
	tapioca_bptree_insert(th, tbpt_id, k, 16, v, 4, BPTREE_INSERT_UNIQUE_KEY);
	tapioca_close(th);
	return 1;
}

int TEST_F(BptreeInterfaceTest, MultiFieldInsertDupe2) 
{
	tapioca_handle *th;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;
	tapioca_bptree_id tbpt_id;
	th = tapioca_open("127.0.0.1", 5555);
	tbpt_id = tapioca_bptree_initialize_bpt_session(th, 100, BPTREE_OPEN_OVERWRITE);
	tapioca_bptree_set_num_fields(th, tbpt_id, 4);
	tapioca_bptree_set_field_info(th, tbpt_id, 0, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	tapioca_bptree_set_field_info(th, tbpt_id, 1, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	tapioca_bptree_set_field_info(th, tbpt_id, 2, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	tapioca_bptree_set_field_info(th, tbpt_id, 3, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	int a, b, c, d;
	a = b = c = d = 1;
	unsigned char k[16];
	unsigned char v[4];
	memcpy(k, &a, 4);
	memcpy(k + 4, &b, 4);
	memcpy(k + 8, &c, 4);
	memcpy(k + 12, &d, 4);
	tapioca_bptree_insert(th, tbpt_id, k, 16, v, 4, BPTREE_INSERT_UNIQUE_KEY);
	d = 2;
	memcpy(k + 12, &d, 4);
	tapioca_bptree_insert(th, tbpt_id, k, 16, v, 4, BPTREE_INSERT_UNIQUE_KEY);
	d = 1;
	memcpy(k + 12, &d, 4);
	tapioca_bptree_insert(th, tbpt_id, k, 16, v, 4, BPTREE_INSERT_UNIQUE_KEY);
	tapioca_close(th); return 1;
}

int TEST_F(BptreeInterfaceTest, MultiFieldInsert) 
{
	// Randomly sample from an array of size keys and insert into our btree
	int i, j, n, r, rv, rv1, rv2;
	char k[5] = "a000";
	char v[5] = "v000";
	char kbuf[18];
	char *kptr;
	tapioca_handle *th;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;
	tapioca_bptree_id tbpt_id;
	th = tapioca_open("127.0.0.1", 5555);
	tbpt_id = tapioca_bptree_initialize_bpt_session(th, 100, BPTREE_OPEN_OVERWRITE);
	tapioca_bptree_set_num_fields(th, tbpt_id, 4);
	tapioca_bptree_set_field_info(th, tbpt_id, 0, 5, BPTREE_FIELD_COMP_STRNCMP);
	tapioca_bptree_set_field_info(th, tbpt_id, 1, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	tapioca_bptree_set_field_info(th, tbpt_id, 2, 5, BPTREE_FIELD_COMP_STRNCMP);
	tapioca_bptree_set_field_info(th, tbpt_id, 3, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);

	for (i = 1; i <= keys; i++)
	{
		k[1] = 0x30 + ((i / 100) % 10);
		k[2] = 0x30 + ((i / 10) % 10);
		k[3] = 0x30 + (i % 10);
		kptr = kbuf;
		memcpy(kptr, k, 5);
		memcpy(kptr + 5, &i, sizeof(int32_t));
		memcpy(kptr + 9, k, 5);
		memcpy(kptr + 14, &i, sizeof(int32_t));

		rv = tapioca_bptree_insert(th, tbpt_id, kbuf, 18, v, 5, BPTREE_INSERT_UNIQUE_KEY);
		if (rv == BPTREE_ERR_DUPLICATE_KEY_INSERTED)
		{
			printf("Tried to insert duplicate key %s\n", k);
		}
		tapioca_commit(th);
		if (i % 250 == 0)
			printf("Inserted %d keys\n", i);
	}

	char v2[10] = "abcd12345";
	for (i = 1; i <= keys / 2; i++)
	{
		k[1] = 0x30 + ((i / 100) % 10);
		k[2] = 0x30 + ((i / 10) % 10);
		k[3] = 0x30 + (i % 10);
		kptr = kbuf;
		memcpy(kptr, k, 5);
		memcpy(kptr + 5, &i, sizeof(int32_t));
		memcpy(kptr + 9, k, 5);
		memcpy(kptr + 14, &i, sizeof(int32_t));

		tapioca_bptree_update(th, tbpt_id, kbuf, 5, v2, 10);
		tapioca_commit(th);
		if (i % 250 == 0)
			printf("Updated %d keys\n", i);
	}

	printf("Verifying ordering... OFF");
//	rv1 = verify_tapioca_bptree_order(th, tbpt_id, BPTREE_VERIFY_RECURSIVELY);
//	rv2 = verify_tapioca_bptree_order(th, tbpt_id, BPTREE_VERIFY_SEQUENTIALLY);
//	dump_tapioca_bptree_contents(th, tbpt_id, 1, 1);
	return (rv1 && rv2);
}

int TEST_F(BptreeInterfaceTest, InsertDupe)
{
	// Randomly sample from an array of size keys and insert into our btree
	int i, j, n, r, rv, rv1, rv2;
	char k[5] = "a000";
	char v[5] = "v000";
	tapioca_handle *th;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;
	tapioca_bptree_id tbpt_id;
	th = tapioca_open("127.0.0.1", 5555);
	tbpt_id = tapioca_bptree_initialize_bpt_session(th, 100, BPTREE_OPEN_OVERWRITE);
	tapioca_bptree_set_num_fields(th, tbpt_id, 1);
	tapioca_bptree_set_field_info(th, tbpt_id, 0, 5, BPTREE_FIELD_COMP_STRNCMP);

	for (i = 1; i <= keys; i++)
	{
		k[1] = 0x30 + ((i / 100) % 10);
		k[2] = 0x30 + ((i / 10) % 10);
		k[3] = 0x30 + (i % 10);
		rv = tapioca_bptree_insert(th, tbpt_id, &k, 5, &v, 5, BPTREE_INSERT_UNIQUE_KEY);
		tapioca_commit(th);
		if (i % 250 == 0)
			printf("Inserted %d keys\n", i);
	}
	// insert the last key we just inserted

	rv = tapioca_bptree_insert(th, tbpt_id, &k, 5, &v, 5, BPTREE_INSERT_UNIQUE_KEY);
	if (rv == BPTREE_ERR_DUPLICATE_KEY_INSERTED)
	{
		printf("Tried to insert duplicate key %s\n", k);
	}
	return rv;
}

int TEST_F(BptreeInterfaceTest, Mget)
{
	// Randomly sample from an array of size keys and insert into our btree
	int i, j, n, r, rv, rv1, rv2;
	char k[5] = "a000";
	char v[5] = "v100";
	tapioca_handle *th;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;

	th = tapioca_open(address, port);
	for (i = 1; i <= keys; i++)
	{
		k[1] = 0x30 + ((i / 100) % 10);
		k[2] = 0x30 + ((i / 10) % 10);
		k[3] = 0x30 + (i % 10);
		rv = tapioca_put(th, &k, 5, &v, 5);
		tapioca_commit(th);
		if (i % 250 == 0) printf("Put %d keys\n", i);
	}


	for (i = 1; i <= keys; i++)
	{
		k[1] = 0x30 + ((i / 100) % 10);
		k[2] = 0x30 + ((i / 10) % 10);
		k[3] = 0x30 + (i % 10);
		rv = tapioca_mget(th, &k, 5);
	}

	mget_result *mres = tapioca_mget_commit(th);
	int mgets = 0;
	char vv[5];
	while(mget_result_count(mres) > 0)
	{
		int bytes = mget_result_consume(mres, vv);
		//printf("Mget sz %d Val %d : %s\n", bytes, mgets, vv);
		mgets++;
	}
	mget_result_free(mres);
	return rv;
}

int main(int argc, char **argv)
{
	int rv, i, k; // , dbug, v, seed, num_threads;
	tapioca_handle *th;
	if (argc < 2)
	{
		printf("%s <Keys to insert> \n", argv[0]);
		exit(-1);
	}
	int keys = atoi(argv[1]);
	int mgets = keys > 10 ? 10 : keys;
	printf("Testing mget interface with %d keys...", mgets);
	rv = test_mget(mgets);
	printf("%s\n", rv ? "pass" : "FAIL");

	printf("Testing multi-field insert...\n");
	rv = test_multi_field_insert(keys);
	printf("%s\n", rv ? "pass" : "FAIL");

	printf("Testing multi-field insert dupe...");
	rv = test_multi_field_insert_dupe(keys);
	printf("%s\n", rv ? "pass" : "FAIL");

	printf("Testing serialization ... ");
	rv = test_serialization();
	printf("%s\n", rv ? "pass" : "FAIL");

	printf("Testing insert / update... ");
	rv = test_update(keys);
	printf("%s\n", rv ? "pass" : "FAIL");

	printf("Testing insert duplicate...");
	rv = test_insert_dupe(keys);
	printf("%s\n", rv ? "pass" : "FAIL");

	exit(0);
}

*/