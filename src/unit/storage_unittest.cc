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
#include "tapiocadb.h"
#include "storage.h"
#include "test_helpers.h"


// A mock consistent hash function such that
// it always returns node id 1 for whatever hash
static int mock_id_for_hash(unsigned int h) {
	return 1;
}


class StorageTest : public testing::Test {
protected:

	virtual void SetUp() {
		tapioca_init_defaults();
		storage_init2(mock_id_for_hash);
	}
	
	virtual void TearDown() { 
		storage_free();
	}
};


TEST_F(StorageTest, GetNotExisting) {
	key* k = createKey(1);
	val* v = NULL;

	v = storage_get(k, 1);
	EXPECT_TRUE(v == NULL);
	EXPECT_EQ(0, storage_key_count());
	EXPECT_EQ(0, storage_val_count());
}


TEST_F(StorageTest, PutGetInteger) {
	key* k = createKey(1);
	val* v = createVal(1, 1);
	val* rv = NULL;
	
	storage_put(k, v, 1, 1);
	rv = storage_get(k, 1);

	EXPECT_FALSE(rv == NULL);
	EXPECT_PRED2(valEqual, rv, v);
	EXPECT_EQ(1, storage_key_count());
	EXPECT_EQ(1, storage_val_count());
	
	key_free(k);
	val_free(v);
	val_free(rv);
}


TEST_F(StorageTest, PutGetString) {
	key* k = createKey("foo");
	val* v = createVal("bar", 1);
	val* rv = NULL;
	
	storage_put(k, v, 1, 1);
	rv = storage_get(k, 1);
	
	EXPECT_FALSE(rv == NULL);
	EXPECT_PRED2(valEqual, rv, v);
	EXPECT_EQ(1, storage_key_count());
	EXPECT_EQ(1, storage_val_count());
	
	key_free(k);
	val_free(v);
	val_free(rv);
}


TEST_F(StorageTest, PutOverwrites) {
	key* k = createKey("foobar");
	val* v1 = createVal("baz", 1);
	val* v2 = createVal("new baz", 1);
	val* rv;
	
	storage_put(k, v1, 1, 1);
	storage_put(k, v2, 1, 1);
	
	rv = storage_get(k, 1);
	EXPECT_FALSE(rv == NULL);
	EXPECT_PRED2(valVersionEqual, rv, v2);
	EXPECT_EQ(1, storage_key_count());
	EXPECT_EQ(1, storage_val_count());

	key_free(k);
	val_free(v1);
	val_free(v2);
	val_free(rv);
}


TEST_F(StorageTest, GetVersions) {
	key* k = createKey(1);
	val* v0 = createVal("v0", 0);
	val* v1 = createVal("v1", 1);
	val* v2a = createVal("v2a", 2);
	val* v2b = createVal("v2b", 2);
	val* v3 = createVal("v3", 3);
	val* v4 = createVal("v4", 4);
	
	storage_put(k, v2a, 1, 1); // Put version 2
	storage_put(k, v2b, 1, 1); // Overwrite version 2
	storage_put(k, v0, 1, 1);  // Put version 0
	storage_put(k, v1, 1, 1);  // Put version 1
		
	EXPECT_PRED3(matchStorageVersion, k, v0, 0);
	EXPECT_PRED3(matchStorageVersion, k, v1, 1);
	EXPECT_PRED3(matchStorageVersion, k, v2b, 2);

	// Should match the newest versions, as versions 3 and 4 do not exist
	EXPECT_PRED3(matchStorageVersion, k, v2b, 3);
	EXPECT_PRED3(matchStorageVersion, k, v2b, 4);

	storage_put(k, v3, 1, 1); // Put version 3
	EXPECT_PRED3(matchStorageVersion, k, v3, 3);
	EXPECT_PRED3(matchStorageVersion, k, v3, 4);
	
	// Version 0 should still be there...
	EXPECT_PRED3(matchStorageVersion, k, v0, 0);
	
	// version 0 should disappear when we put the 5th version
	storage_put(k, v4, 1, 1);
	EXPECT_PRED3(matchStorageVersion, k, v3, 3);
	EXPECT_PRED3(matchStorageVersion, k, v4, 4);
	EXPECT_PRED3(matchStorageVersion, k, (val*)NULL, 0);
	
	EXPECT_EQ(1, storage_key_count());
	EXPECT_EQ(StorageMaxOldVersions, storage_val_count());
	
	key_free(k);
	val_free(v0);
	val_free(v1);
	val_free(v2a);
	val_free(v2b);
	val_free(v3);
	val_free(v4);
}


TEST_F(StorageTest, Stress) {
	key* k;
	val* v;
	int i, n = 1000000;
	
	for (i = 0; i < n; i++) {
		k = createKey(i);
		v = createVal(i, 1);
		EXPECT_TRUE(storage_put(k, v, 1, 1) == 1);
		key_free(k);
		val_free(v);
	}
	
	for (i = 0; i < n; i++) {
		k = createKey(i);
		v = createVal(i, 1);
		EXPECT_PRED3(matchStorageVersion, k, v, 1);
		key_free(k);
		val_free(v);
	}
	
	EXPECT_EQ(n, storage_key_count());
	EXPECT_EQ(n, storage_val_count());
}


TEST_F(StorageTest, StorageMaxOldVersions) {
	key* k;
	val* v;
	int n = 2*StorageMaxOldVersions;
	
	for (int i = 0; i < n; i++) {
		k = createKey("foo");
		v = createVal(i, i);
		EXPECT_EQ(1, storage_put(k, v, 1, 1));
		key_free(k);
		val_free(v);
	}
	
	EXPECT_EQ(1, storage_key_count());
	EXPECT_EQ(StorageMaxOldVersions, storage_val_count());
}


TEST_F(StorageTest, KeyBecomesLocal) {
	key* k;
	val* v;
	int local = 1, not_local = 0;
	
	k = createKey("bar");
	for (int i = 0; i < 10; i++) {
		v = createVal(i, i);
		EXPECT_EQ(1, storage_put(k, v, not_local, 1));
		val_free(v);
	}
	
	v = createVal(10, 1000);
	EXPECT_EQ(1, storage_put(k, v, local, 1));
	val_free(v);
	
	int size = storage_get_current_size();
	int keys = storage_key_count();
	int vals = storage_val_count();
	
	// gc everything
	storage_gc_at_least(1024*1024*1024);
	
	EXPECT_EQ(size, storage_get_current_size());
	EXPECT_EQ(keys, storage_key_count());
	EXPECT_EQ(vals, storage_val_count());
}


TEST_F(StorageTest, GCSimple) {
	key* k;
	val* v;
	int i, n = 100000;
	
	for (i = 0; i < n; i++) {
		k = createKey(i);
		v = createVal(i, 1);
		EXPECT_TRUE(storage_put(k, v, 0, 1) == 1);
		key_free(k);
		val_free(v);
	}
	
	EXPECT_EQ(n, storage_key_count());
	EXPECT_EQ(n, storage_val_count());
	
	storage_gc_at_least(1024*1024*1024);
	
	EXPECT_EQ(1, storage_gc_count());
	EXPECT_EQ(0, storage_key_count());
	EXPECT_EQ(0, storage_val_count());
}



TEST_F(StorageTest, GCPreserveNullVal) {
	key* k;
	val* v;
	k = createKey(331);
	
	// deliver version 3164
	v = createVal(54, 3164);
	EXPECT_EQ(storage_put(k,v,0,0), 1);
	
	// should not be cached...
	v = storage_get(k, 3445);
	EXPECT_TRUE(v == NULL);
	
	// remote get version 3379, this puts a null val into storage
	v = val_new(NULL, 0);
	EXPECT_EQ(storage_put(k,v,0,1), 1);
	
	// remote put version 3164
	v = createVal(54, 3164);
	EXPECT_EQ(storage_put(k,v,0,1), 1);

	// deliver version 3381
	v = createVal(55, 3381);
	EXPECT_EQ(storage_put(k,v,0,0), 1);
	
	// get max version 3390
	v = storage_get(k, 3390);
	EXPECT_EQ(v->version, 3381);
	
	storage_gc_stop();
	storage_gc_at_least(1024*1024*1024);
	storage_gc_start();
	
	// deliver version 3391
	v = createVal(56, 3391);
	EXPECT_EQ(storage_put(k,v,0,0), 1);
	
	// should be cached! i.e. gc should preserve null val
	
	// remote put version 3164
	v = createVal(54, 3164);
	EXPECT_EQ(storage_put(k,v,0,1), 1);
	
	// get max version 3445
	v = storage_get(k, 3445);
	EXPECT_EQ(v->version, 3391);
}
