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


#include "gtest.h"
#define LARGE_BUFFER_SZ 128000

extern "C" {
	#include "bplustree.h"
}

#include "tapiocadb.h"
#include "transaction.h"
#include "storage.h"
#include "test_helpers.h"

class BptreeTestBase: public testing::Test {

protected:
    bptree_session *bps;
	tapioca_bptree_id tbpt_id;
	enum bptree_open_flags open_flags;
	
	int rv, rv1, rv2, keys;
	char k[LARGE_BUFFER_SZ];
	char v[LARGE_BUFFER_SZ];
	int32_t ksize, vsize;
	const char *hostname;
	int port;
	bool DBUG;
	bool local_storage = true; // whether we connect to an external or our own
    tapioca_handle* th;

	void insertSampleData() {
		
		strncpy(k, "a000", 5);
		strncpy(v, "v000", 5);
		for (int i = 1; i <= keys; i++)
		{
			sprintf(k,"a%03d",i);
			sprintf(v,"v%03d",i);
			rv = tapioca_bptree_insert(th, tbpt_id, &k, 5, &v, 5);
			ASSERT_EQ(rv, BPTREE_OP_SUCCESS);
			rv = tapioca_commit(th);
			EXPECT_GE(0, rv);
			if (i % 250 == 0) printf("Inserted %d keys\n", i);
		}

	}
	
	tapioca_bptree_id createNewTree(tapioca_bptree_id tbpt_id) {
		open_flags = BPTREE_OPEN_OVERWRITE;
		bps = (bptree_session *) malloc(sizeof(bptree_session));
		rv = tapioca_bptree_initialize_bpt_session(th, tbpt_id, open_flags,
												   BPTREE_INSERT_UNIQUE_KEY);
		this->tbpt_id = tbpt_id;
		EXPECT_EQ(tbpt_id, rv);
	}
	
	virtual void SetUp() {
		if (local_storage) 
		{
			system("killall -q -9 cm tapioca acceptor proposer rec");
			system("cd ..; bash scripts/launch_all.sh --kill-all --clear-db > /dev/shm/mosql-tst.log; cd -");
			sleep(1);
		}
		memset(k, 0,LARGE_BUFFER_SZ);
		memset(v, 0,LARGE_BUFFER_SZ);
		hostname = "127.0.0.1";
		port = 5555;
		keys = 1000;
		DBUG = false;
		// Create a default tree for all test cases; some may create their own
        th = tapioca_open(hostname, port);
        EXPECT_NE(th, (tapioca_handle*)NULL);
		createNewTree(1000);
	}
	
	virtual void TearDown() {
		
		if (DBUG) {
			tapioca_bptree_debug(th,tbpt_id, BPTREE_DEBUG_DUMP_GRAPHVIZ);
			tapioca_bptree_debug(th,tbpt_id, BPTREE_DEBUG_DUMP_RECURSIVELY);
			tapioca_bptree_debug(th,tbpt_id, BPTREE_DEBUG_DUMP_SEQUENTIALLY);
			tapioca_bptree_debug(th,tbpt_id, BPTREE_DEBUG_VERIFY_RECURSIVELY);
			tapioca_bptree_debug(th,tbpt_id, BPTREE_DEBUG_VERIFY_SEQUENTIALLY);
			tapioca_bptree_debug(th,tbpt_id, BPTREE_DEBUG_INDEX_RECURSIVE_SCAN);
		}

        tapioca_close(th);
		if (local_storage) 
		{
			system("killall -q cm tapioca example_acceptor example_proposer rec");
			sleep(2);
			system("killall -q -9 cm tapioca example_acceptor example_proposer rec");
		}
	}

};

