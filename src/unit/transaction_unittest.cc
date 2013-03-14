#include "gtest.h"
#include "tapiocadb.h"
#include "transaction.h"
#include "storage.h"
#include "test_helpers.h"

class TransactionTest : public testing::Test {
protected:
	
	transaction* t;
 	int storage_items;
	
	virtual void SetUp() {
		tapioca_init_defaults();
		t = transaction_new();
		SetUpStorage();
	}
	
	virtual void TearDown() {
		transaction_destroy(t);
	}
	
	void SetUpStorage() {
		key* k;
		val* v;
		
		storage_init();
		storage_items = 1000;
		
		for (int i = 0; i < storage_items; i++) {
			k = createKey(i);
			v = createVal(i, 0);
			storage_put(k, v, 1, 1);
			key_free(k);
			val_free(v);
		}
	}
};


TEST_F(TransactionTest, ReadOnly) {
	key* k;
	val* v;
	int n = 1000;

	EXPECT_EQ(1, transaction_read_only(t));

	for (int i = 0; i < n; i++) {
		k = createKey(i);
		v = transaction_get(t, k);
		EXPECT_PRED3(matchStorageVersion, k, v, 0);
	}
	
	EXPECT_EQ(1, transaction_read_only(t));
}
