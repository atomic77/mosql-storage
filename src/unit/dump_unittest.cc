#include "gtest.h"
#include "tapiocadb.h"
#include "storage.h"
#include "test_helpers.h"


// A mock consistent hash function such that
// it always returns node id 1 for whatever hash
static int mock_id_for_hash(unsigned int h) {
	return 0;
}


class DumpTest : public testing::Test {
protected:

	virtual void SetUp() {
		tapioca_init_defaults();
		storage_init2(mock_id_for_hash);
	}
	
	virtual void TearDown() { 
		storage_free();
	}
};


void iter(key* k, val* v, void* arg) {
	EXPECT_EQ((*(int*)k->data) * 2, *(int*)v->data);
}


TEST_F(DumpTest, Iterate) {
	key* k;
	val* v;
	int i, n = 100000;
	
	for (i = 0; i < n; i++) {
		k = createKey(i);
		v = createVal(2*i, 1);
		EXPECT_TRUE(storage_put(k, v, 1, 1) == 1);
		key_free(k);
		val_free(v);
	}
	EXPECT_EQ(n, storage_iterate(1, iter, NULL));
}


TEST_F(DumpTest, IterateSkipVersions) {
	key* k;
	val* v;
	int some_key = 12341;
	
	k = createKey(some_key);

	for (int i = 0; i < 10; i++) {
		v = createVal(i, i);
		EXPECT_EQ(storage_put(k, v, 1, 1), 1);
		val_free(v);
	}
	
	v = createVal(2*some_key, 11);
	EXPECT_EQ(storage_put(k, v, 1, 1), 1);
	val_free(v);
	
	EXPECT_EQ(1, storage_iterate(11, iter, NULL));
}
