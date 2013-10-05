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
#include "gtest.h"
extern "C" {
	#include "bplustree.h"
}

class BptreeBasicTest : public testing::Test {

protected:
    //tapioca_handle* th;
    
	virtual void SetUp() {
		//system("cd ..; ./start.sh > /dev/null; cd unit");
	}
	
	virtual void TearDown() {
	}

};

TEST_F(BptreeBasicTest, BasicNodeSerDe) {
	size_t bsize1, bsize2, bsize3, bsize4;
	int c;
	void *buf, *buf2;
	bptree_node *n, *n2;
	n = create_new_empty_bptree_node();
	n->key_count = 2;
	uuid_generate_random(n->self_key);
	n->leaf = 0;
	uuid_generate_random(n->children[0]);
	uuid_generate_random(n->children[1]);
	uuid_generate_random(n->children[2]);
	n->key_sizes[0] = 5;
	n->key_sizes[1] = 7;
	n->keys[0] = (unsigned char *) malloc(5);
	n->keys[1] = (unsigned char *) malloc(7);
	strncpy(n->keys[0], "aaaa",4);
	strncpy(n->keys[1], "aaaaaa", 6);
	
	n->value_sizes[0] = 3;
	n->value_sizes[1] = 9;
	n->values[0] = (unsigned char *) malloc(3);
	n->values[1] = (unsigned char *) malloc(9);
	strncpy(n->values[0] , "ccc", 3);
	strncpy(n->values[1] , "dddddddd", 10);
	buf = marshall_bptree_node(n, &bsize1);
	n2 = unmarshall_bptree_node(buf, bsize1, &bsize3);
	buf2 = marshall_bptree_node(n2, &bsize2);
	c = memcmp(buf, buf2, bsize1);
	// The buffers buf and buf2 should now be identical
	EXPECT_TRUE (bsize1 > 0);
	EXPECT_TRUE (bsize1 == bsize2);
	EXPECT_TRUE (c == 0);

}

TEST_F(BptreeBasicTest, BasicMetaNodeSerDe) {
	size_t bsize1, bsize2;
	int c;
	void *buf, *buf2;
	bptree_meta_node m, *m2;
	memset(&m, 0, sizeof(bptree_meta_node));
	m.execution_id = 1231231;
	uuid_generate_random(m.root_key);
	m.bpt_id = 123;
	
	buf = marshall_bptree_meta_node(&m, &bsize1);
	m2 = unmarshall_bptree_meta_node(buf, bsize1);
	buf2 = marshall_bptree_meta_node(m2, &bsize2);
	c = memcmp(buf, buf2, bsize1);
	// The buffers buf and buf2 should now be identical
	EXPECT_TRUE (bsize1 > 0);
	EXPECT_TRUE(bsize1 == bsize2);
	EXPECT_TRUE(c == 0);

}
