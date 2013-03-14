#include "btree.h"
#include <string.h>
#include <assert.h>


static long btree_key = 0;


static int btree_update_recursive(transaction* t, btree_node* n, long k, long v);
static int btree_search_recursive(transaction* t, btree_node* n, long k, long* v);
static int btree_insert_nonfull(transaction* t, btree_node* x, long k, long v);
static int btree_range_recursive(transaction* t, btree_node* n, long min, long max, long* range, long* range_count);

static int btree_split_child(transaction* t, btree_node* x, int i, btree_node* y);

static int node_is_full(btree_node * n);
static int read_btree(transaction* t, btree* b);
static int read_btree_or_create(transaction* t, btree* b);
static int write_btree(transaction* t, btree* b);
static int read_node(transaction* t, long node_key, btree_node* n);
static int write_node(transaction* t, btree_node* n);
static long get_unique_key();


int btree_search(transaction* t, long k, long* v) {
	int rv;
	btree b;
    btree_node root;

	rv = read_btree(t, &b);
	if (rv <= 0) return rv;
	
	rv = read_node(t, b.root_key, &root);
	if (rv <= 0) return rv;
    
    return btree_search_recursive(t, &root, k, v);
}


int btree_update(transaction* t, long k, long v) {
    int rv;
    btree b;
    btree_node root;
    
    rv = read_btree(t, &b);
    if (rv <= 0) return rv;
    
    rv = read_node(t, b.root_key, &root);
    if (rv <= 0) return rv;
    
    return btree_update_recursive(t, &root, k, v);
}


int btree_range(transaction* t, long min, long max, long* range, long* range_count) {
	int rv;
    btree b;
    btree_node root;
    
	rv = read_btree(t, &b);
	if (rv <= 0) return rv;
	
	rv = read_node(t, b.root_key, &root);
	if (rv <= 0) return rv;

	(*range_count) = 0;
	
	return btree_range_recursive(t, &root, min, max, range, range_count);
}


int btree_insert(transaction* t, long k, long v) {
	int rv;
	btree b;
    btree_node root;
    
	rv = read_btree_or_create(t, &b);
	if (rv == -1) return -1;
	    
    if (read_node(t, b.root_key, &root) < 0)
        return -1;
    
    if (node_is_full(&root)) {
        //Make new root node
        btree_node new;
        memset(&new, '\0', sizeof(btree_node));
        new.leaf = 0;
        new.key_count = 0;
        new.children[0] = b.root_key;
        new.self_key = get_unique_key();

        //Update root pointer
        b.root_key = new.self_key;
        if (write_btree(t, &b) < 0)
            return -1;
        
        if (btree_split_child(t, &new, 0, &root) < 0)
            return -1;
            
        return btree_insert_nonfull(t, &new, k, v);
        
    } else {
        return btree_insert_nonfull(t, &root, k, v);
    }
}


static int btree_split_child(transaction* t, btree_node* x, int i, btree_node* y) {
    int j;
    btree_node new;
    
    memset(&new, '\0', sizeof(btree_node));
    new.leaf = y->leaf;
    new.key_count = BTREE_MIN_DEGREE - 1;
    new.self_key = get_unique_key();

    for (j = 0; j <= BTREE_MIN_DEGREE - 2; j++) {
		new.keys[j] = y->keys[j+BTREE_MIN_DEGREE];
		new.values[j] = y->values[j+BTREE_MIN_DEGREE];
    }
    
    if (!y->leaf) {
        for (j = 0; j <= BTREE_MIN_DEGREE - 1; j++) {
            new.children[j] = y->children[j+BTREE_MIN_DEGREE];
        }
    }
    y->key_count = BTREE_MIN_DEGREE - 1;
    

    for (j = x->key_count; j > i; j--) {
        x->children[j+1] = x->children[j];
    }
    x->children[i+1] = new.self_key;
    
    for (j = x->key_count - 1; j >= i; j--) {
		x->keys[j+1] = x->keys[j];
		x->values[j+1] = x->values[j];
    }
    
	x->keys[i] = y->keys[BTREE_MIN_DEGREE-1];
	x->values[i] = y->values[BTREE_MIN_DEGREE-1];
    x->key_count++;
    
    if (write_node(t, x) < 0)
        return -1;
        
    if (write_node(t, y) < 0)
        return -1;
    
    if (write_node(t, &new) < 0)
        return -1;
    
    return 0;
}


static int btree_insert_nonfull(transaction* t, btree_node* x, long k, long v) {
    int i;
    
    i = x->key_count - 1;
    if (x->leaf) {
        while (i >= 0 && k < x->keys[i]) {
			x->keys[i+1] = x->keys[i];
			x->values[i+1] = x->values[i];
            i--;
        }

		x->keys[i+1] = k;
		x->values[i+1] = v;
        x->key_count++;
        return write_node(t, x);

    } else {
        btree_node n;
        
		while (i >= 0 && k < x->keys[i])
            i--;
            
        i++;

        if (read_node(t, x->children[i], &n) < 0)
            return -1;
            
        if (node_is_full(&n)) {
            return btree_split_child(t, x, i, &n);
			if (k != x->keys[i]) {
				if (read_node(t, x->children[i+1], &n) < 0)
                    return -1;
			}
        }
        
        return btree_insert_nonfull(t, &n, k, v);   
    }
}


static int btree_search_recursive(transaction* t, btree_node* n, long k, long* v) {
    int i = 0;
    btree_node next;
    
    while ((i < n->key_count) && (n->keys[i] < k))
        i++;
        
    if ((i < n->key_count) && (k == n->keys[i])) {
		*v = n->values[i];
        return 1;
    }

    if (n->leaf)
        return 0;

    if (read_node(t, n->children[i], &next) < 0)
        return -1;
    
    return btree_search_recursive(t, &next, k, v);
}


static int btree_range_recursive(transaction* t, btree_node* n, long min, long max, long* range, long* range_count) {
    int i = 0;
    btree_node child;

    while ((i < n->key_count) && (n->keys[i] < min))
        i++;
    
    while ((i < n->key_count) && (n->keys[i] <= max)) {
		range[*range_count] = n->keys[i];
		(*range_count)++;
		
        if (!n->leaf) {
            if (read_node(t, n->children[i], &child) < 0)
                return -1;
            if (btree_range_recursive(t, &child, min, max, range, range_count) < 0)
                return -1;
        }
        
        i++;
    }
    
    if (!n->leaf) {
        if (read_node(t, n->children[i], &child) < 0)
            return -1;
        if (btree_range_recursive(t, &child, min, max, range, range_count) < 0)
            return -1;
    }

    return 1;
}


static int btree_update_recursive(transaction* t, btree_node* n, long k, long v) {
    int i = 0;
    btree_node next;
    
    while ((i < n->key_count) && (n->keys[i] < k))
        i++;
        
    if ((i < n->key_count) && (k == n->keys[i])) {
        n->values[i] = v;
        write_node(t, n);
        return 1;
    }

    if (n->leaf)
        return 0;

    if (read_node(t, n->children[i], &next) < 0)
        return -1;
    
    return btree_update_recursive(t, &next, k, v);
}


static int node_is_full(btree_node* n) {
    return (n->key_count == ((BTREE_MIN_DEGREE*2) - 1));
}


static int read_btree(transaction* t, btree* b) {
    key k;
    val* v;
    
    k.size = sizeof(long);
    k.data = &btree_key;
    
    v = transaction_get(t, &k);
	if (v == NULL)
		return -1;
	if (v->size == 0)
		return 0;

    assert(sizeof(btree) == v->size);
    memcpy(b, v->data, v->size);
    val_free(v);
    
    return 1;
}


static int read_btree_or_create(transaction* t, btree* b) {
	int rv;
	rv = read_btree(t, b);
	if (rv < 0) return -1;
	if (rv == 0) {
		btree tree;
		btree_node root;
		tree.root_key = 1;
		root.key_count = 0;
		root.leaf = 1;
		root.self_key = tree.root_key;
		write_node(t, &root);
		write_btree(t, &tree);
	}
	return 1;
}


static int write_btree(transaction* t, btree* b) {
    key k;
    val v;
    
    k.size = sizeof(long);
    k.data = &btree_key;
    
    v.size = sizeof(btree);
    v.data = b;
    
    return transaction_put(t, &k, &v);
}


static int read_node(transaction* t, long node_key, btree_node* n) {
    key k;
    val* v;
    
    k.size = sizeof(long);
    k.data = &node_key;
    
    v = transaction_get(t, &k);
    if (v == NULL)
        return -1;

    assert(sizeof(btree_node) == v->size);
    
    memcpy(n, v->data, v->size);
    assert(node_key == n->self_key);
    val_free(v);
    
    return 1;
}


static int write_node(transaction* t, btree_node* n) {
    key k;
    val v;
    
    k.size = sizeof(long);
    k.data = &n->self_key;
    
    v.size = sizeof(btree_node);
    v.data = n;
    
    return transaction_put(t, &k, &v);
}


static long get_unique_key() {
    static long ctr = 1;
    return ((ctr++) << 8) | NodeID;
}
