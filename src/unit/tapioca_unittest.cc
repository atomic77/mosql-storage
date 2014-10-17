/*
    Copyright (C) 2013 University of Lugano

	This file is part of the MoSQL storage system. 

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

#include <gtest/gtest.h>
#include "tapioca.h"
#include "remote_mock.h"
#include "test_helpers.h"

class TapiocaTest : public testing::Test {

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

static int tapioca_put_int(tapioca_handle* th, int k, int v) {
	return tapioca_put(th, &k, sizeof(int), &v, sizeof(int));
}


static int tapioca_get_int(tapioca_handle* th, int k, int* v) {
	return tapioca_get(th, &k, sizeof(int), v, sizeof(int));
}


TEST_F(TapiocaTest, GetNotExisting) {
	int v = 0;
	int rv = 0;
	
	rv = tapioca_get_int(th, 1234, &v);
	
	EXPECT_EQ(v, 0);
	EXPECT_EQ(rv, 0);
}


TEST_F(TapiocaTest, GetExisting) {
	int v = 0;
	int rv = 0;

	rv = tapioca_put_int(th, 1234, 4321);
	EXPECT_EQ(rv, 1);
	
	rv = tapioca_get_int(th, 1234, &v);
	EXPECT_EQ(v, 4321);
	EXPECT_EQ(rv, (int)sizeof(int));
}


TEST_F(TapiocaTest, GetCommitted) {
	int v = 0;
	int rv = 0;

	rv = tapioca_put_int(th, 1234, 4321);
	EXPECT_EQ(rv, 1);
	
	rv = tapioca_commit(th);
	EXPECT_GE(rv, 0);
	
	rv = tapioca_get_int(th, 1234, &v);
	EXPECT_EQ(v, 4321);
	EXPECT_EQ(rv, (int)sizeof(int));
	
	rv = tapioca_commit(th);
	EXPECT_GE(rv, 0);
}


TEST_F(TapiocaTest, MGetSimple) {
    mget_result* result;
    int iterations = 64;
    
    for (int i = 0; i < iterations; i++) {
        EXPECT_EQ(1, tapioca_put_int(th, i, i));
    }
    EXPECT_NE(tapioca_commit(th), -1);
    
    for (int i = 0; i < iterations; i++) {
        tapioca_mget(th, &i, sizeof(int));
    }
    
    result = tapioca_mget_commit(th);
    EXPECT_EQ(iterations, mget_result_count(result));
    
    int i = 0;
    int v, size;
    while (mget_result_count(result) > 0) {
        size = mget_result_consume(result, &v);
        EXPECT_EQ((int)sizeof(int), size);
        EXPECT_EQ(i++, v);
    }
    
    mget_result_free(result);
}



TEST_F(TapiocaTest, MGetNotExisting) {
    mget_result* result;
    int iterations = 32;
    
    for (int i = 0; i < iterations; i++) {
        EXPECT_EQ(1, tapioca_put_int(th, i, i));
    }
    EXPECT_NE(tapioca_commit(th), -1);
    
    for (int i = 0; i < (iterations+1); i++) {
        tapioca_mget(th, &i, sizeof(int));
    }
    
    result = tapioca_mget_commit(th);
    EXPECT_EQ((iterations+1), mget_result_count(result));
    
    int v, size;
    for (int i = 0; i < iterations; i++) {
        size = mget_result_consume(result, &v);
        EXPECT_EQ((int)sizeof(int), size);
        EXPECT_EQ(i, v);
    }
    
    size = mget_result_consume(result, &v);
    EXPECT_EQ(size, 0);
    
    mget_result_free(result);
}


TEST_F(TapiocaTest, GetLarge) {
	char* v;
	char* check;
	int vsize = 60*1024;
	int iterations = 5;
	
	v = (char*)malloc(vsize);
	check = (char*)malloc(vsize);
	
 	for (int i = 0; i < iterations; i++) {
		memset(v, i, vsize);
		EXPECT_EQ(1, tapioca_put(th, &i, sizeof(int), v, vsize));
		EXPECT_NE(tapioca_commit(th), -1);
	}
	
	for (int i = 0; i < iterations; i++) {
		memset(check, i, vsize);
		EXPECT_EQ(vsize, tapioca_get(th, &i, sizeof(int), v, vsize));
		EXPECT_GE(tapioca_commit(th), 0);
		EXPECT_TRUE(memcmp(check, v, vsize) == 0);
	}
	
	free(v);
	free(check);
}


TEST_F(TapiocaTest, TransactionTooBig) {
	char v[10000];
	
	for (int i = 0; i < 7; i++) {
		EXPECT_EQ(1, tapioca_put(th, &i, sizeof(int), v, 10000));
	}
	EXPECT_EQ(-1, tapioca_commit(th));
	
	int i = 0;
	EXPECT_EQ(0, tapioca_get(th, &i, sizeof(int), v, 10000));
}


TEST_F(TapiocaTest, ConflictingTransactions) {
	int v = 0;
	int rv = 0;

	tapioca_handle* th2;
	
	th2 = tapioca_open("127.0.0.1", 5555);
	EXPECT_NE(th2, (tapioca_handle*)NULL);

	// We initialize tapioca with some default k-v
	rv = tapioca_put_int(th, 1234, 4321);
	EXPECT_EQ(rv, 1);
	rv = tapioca_commit(th);
	EXPECT_GE(rv, 0);
	
	// t1: R(1234) W(1234)
	// t2: R(1234) W(1234)
	rv = tapioca_get_int(th, 1234, &v);
	EXPECT_EQ(v, 4321);
	rv = tapioca_put_int(th, 1234, 1);
	EXPECT_EQ(rv, 1);
	
	rv = tapioca_get_int(th2, 1234, &v);
	EXPECT_EQ(v, 4321);
	rv = tapioca_put_int(th2, 1234, 2);
	EXPECT_EQ(rv, 1);
	
	// t1 commits
	rv = tapioca_commit(th);
	EXPECT_GE(rv, 0);
	
	// t2 aborts
	rv = tapioca_commit(th2);
	EXPECT_EQ(rv, -1);
	
	// We should see t1's write
	tapioca_get_int(th2, 1234, &v);
	EXPECT_EQ(v, 1);

	tapioca_close(th2);
}


TEST_F(TapiocaTest, Rollback) {
	int v = 0;
	int count = 10;
	
	for (int i = 0; i < count; i++) {
	    EXPECT_EQ(1, tapioca_put_int(th, i, i));
	}
	EXPECT_EQ(1, tapioca_rollback(th));

	for (int i = 0; i < count; i++) {
		EXPECT_EQ(0, tapioca_get_int(th, i, &v));
		EXPECT_EQ(v, 0);
	}
}

/* TODO Re-enable this test
TEST_F(TapiocaTest, RecoverySimple) {
	key* k;
	val *v1, *v2;
	int txs = 3;
	int puts = 10;
	struct remote_mock* rm;

    sleep(1);    // TODO remove this sleep!!!
	rm = remote_mock_new();
	EXPECT_NE(rm, (remote_mock*)NULL);
	
	int counter = 0;
	for (int i = 0; i < txs; i++) {
		for (int j = 0; j < puts; j++) {
			EXPECT_EQ(tapioca_put_int(th, counter, counter), 1);
			counter++;
		}
		EXPECT_EQ(tapioca_commit(th), 0);
	}
	
	for (int i = 0; i < counter; i++) {
		k = createKey(i);
		v1 = createVal(i, 1);
		v2 = mock_recover_key(rm, k);
		EXPECT_PRED2(valEqual, v1, v2);
		key_free(k);
		val_free(v1);
		val_free(v2);
	}

	remote_mock_free(rm);
}
*/