#include "gtest.h"
#include "tapiocadb.h"
#include "vset.h"
#include "test_helpers.h"


class VsetTest : public testing::Test {
protected:
	
	vset vs;
	
	virtual void SetUp() {
		tapioca_init_defaults();
		vs = vset_new();
	}
	
	virtual void TearDown() {
		vset_free(vs);
	}
};


TEST_F(VsetTest, GetEmpty) {
	EXPECT_TRUE(vset_get(vs, 1) == NULL);
	EXPECT_EQ(0, vset_count(vs));
}


TEST_F(VsetTest, GetExisting) {
	int i;
	val* v;
	val* check;
	
	for (i = 0; i < StorageMaxOldVersions; i++) {
		v = createVal(i, i);
		EXPECT_EQ(0, vset_add(vs, v));
		val_free(v);
	}
	
	for (i = 0; i < StorageMaxOldVersions; i++) {
		v = vset_get(vs, i);
		check = createVal(i, i);
		EXPECT_PRED2(valVersionEqual, v, check);
		val_free(v);
		val_free(check);
	}
	
	EXPECT_EQ(StorageMaxOldVersions, vset_count(vs));
}


TEST_F(VsetTest, GetTooOld) {
	val* v;
	int count = 2*StorageMaxOldVersions;
	
	for (int i = 0; i < count; i++) {
		v = createVal(i, i);
		EXPECT_EQ(0, vset_add(vs, v));
		val_free(v);
	}
	
	EXPECT_EQ(StorageMaxOldVersions, vset_count(vs));
	
	for (int i = 0; i < StorageMaxOldVersions; i++) {
		v = vset_get(vs, i);
		EXPECT_TRUE(v == NULL);
	}
}


TEST_F(VsetTest, AllocatedBytes) {
	val* v;
	std::string s;
	int bytes = vset_allocated_bytes(vs);
	
	s.assign(16384, 'a');
	for (int i = 0; i < StorageMaxOldVersions; i++) {
			v = createVal(s, i);
			EXPECT_EQ(vset_add(vs, v), 0);
			val_free(v);
			EXPECT_EQ((bytes + 16384), vset_allocated_bytes(vs));
			bytes = vset_allocated_bytes(vs);
	}
}
