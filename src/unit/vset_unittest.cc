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
