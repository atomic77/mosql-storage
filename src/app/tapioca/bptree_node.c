

#include "bptree_node.h"
#include <msgpack.h>
#include <assert.h>


typedef struct bptree_node {
	uuid_t self_key;
	int16_t key_count;
	int16_t leaf;
	uuid_t parent;
	uuid_t prev_node;
	uuid_t next_node;
	int16_t node_active;
	char active[BPTREE_NODE_SIZE];
	int32_t key_sizes[BPTREE_NODE_SIZE];
	int32_t value_sizes[BPTREE_NODE_SIZE];
	unsigned char *keys[BPTREE_NODE_SIZE];
	unsigned char *values[BPTREE_NODE_SIZE];
	// TODO Dynamically allocate this based on whether this is a leaf or not
	uuid_t children[BPTREE_NODE_SIZE + 1];
} bptree_node;


unsigned char * bpnode_get_id(bptree_node *x)
{
	return x->self_key;
}


unsigned char * bpnode_get_child_id(bptree_node* x, int c_pos)
{
	return x->children[c_pos];
}

unsigned char * bpnode_get_parent_id(bptree_node *x)
{
	return x->parent;
}
unsigned char * bpnode_get_next_id(bptree_node *x)
{
	return x->next_node;
}
unsigned char * bpnode_get_prev_id(bptree_node *x)
{
	return x->prev_node;
}
int bpnode_size(bptree_node *x) 
{
	return x->key_count;
}

int bpnode_is_eof(bptree_node *x)
{
	return (x->leaf && uuid_is_null(x->next_node));
}

int bpnode_is_leaf(bptree_node *x)
{
	return x->leaf;
}

int bpnode_set_active(bptree_node *x, int pos)
{
	x->active[pos] = 1;
}
int bpnode_set_inactive(bptree_node *x, int pos)
{
	x->active[pos] = 0;
}

int bpnode_set_leaf(bptree_node *x, int leaf)
{
	x->leaf = leaf;
}

void bpnode_size_inc(bptree_node *x)
{
	x->key_count++;
}

void bpnode_size_dec(bptree_node *x)
{
	x->key_count--;
}

void bpnode_set_next(bptree_node *x, bptree_node *src)
{
	uuid_copy(x->next_node, src->self_key);
}

void bpnode_set_prev(bptree_node *x, bptree_node *src)
{
	uuid_copy(x->prev_node, src->self_key);
}

void bpnode_set_parent(bptree_node *c, bptree_node *p)
{
	uuid_copy(c->parent, p->self_key);
}

void bpnode_clear_parent(bptree_node *x)
{
	uuid_clear(x->parent);
}

void bpnode_clear_child(bptree_node *x, int c)
{
	uuid_clear(x->children[c]);
}

unsigned char *bpnode_get_key(bptree_node *x, int pos)
{
	return x->keys[pos];
}

unsigned char *bpnode_get_value(bptree_node *x, int pos)
{
	return x->values[pos];
}

int bpnode_get_key_size(bptree_node *x, int pos)
{
	return x->key_sizes[pos];
}

int bpnode_get_value_size(bptree_node *x, int pos)
{
	return x->value_sizes[pos];
}



int bpnode_is_same(bptree_node *x, bptree_node *y)
{
	if (x == NULL || y == NULL) return 0;
	return (uuid_compare(x->self_key, y->self_key) == 0);
}

int bpnode_is_empty(bptree_node * n)
{
	return n->key_count == 0;
}
int bpnode_is_active(bptree_node *x, int pos)
{
	return x->active[pos];
}
void bpnode_set_child(bptree_node *x, int pos, bptree_node *c)
{
	uuid_copy(x->children[pos], c->self_key);
}

void bpnode_set_child_id(bptree_node *x, int pos, uuid_t id)
{
	uuid_copy(x->children[pos], id);
}

void bpnode_set_next_id(bptree_node *x, uuid_t id)
{
	uuid_copy(x->next_node, id);
}

int bpnode_is_node_active(bptree_node *x)
{
	return x->node_active;
}

/*@ Shift the elements of bptree_node right at position pos*/
void shift_bptree_node_elements_right(bptree_node *x, int pos)
{
	assert(x->key_count <= BPTREE_NODE_SIZE);
	int j;
	if(pos > x->key_count-1) return;
	for (j = x->key_count - 1; j >= pos; j--)
	{
		copy_node_data(x, j, 1);
	}
}

void shift_bptree_node_children_right(bptree_node *x, int pos)
{
	int j;
	for (j = x->key_count ; j >= pos; j--)
	{
		uuid_copy(x->children[j + 1],x->children[j]);
	}
}

/*@ Shift the elements of bptree_node right at position pos*/
void shift_bptree_node_elements_left(bptree_node *x, int pos)
{
	assert(x->key_count <= BPTREE_NODE_SIZE && pos > 0);
	int j;
	if(pos > x->key_count-1) return;
	for (j = pos; j < x->key_count; j++)
	{
		copy_node_data(x, j, -1);
	}
}

void shift_bptree_node_children_left(bptree_node *x, int pos)
{
	int j;
	for (j = pos; j <= x->key_count; j++)
	{
		uuid_copy(x->children[j - 1],x->children[j]);
	}
}

void clear_key_position(bptree_node *x, int i)
{
	free(x->keys[i]);
	free(x->values[i]);
	x->keys[i] = NULL;
	x->values[i] = NULL;
	x->key_sizes[i] = -1;
	x->value_sizes[i] = -1;
}

void delete_key_from_node(bptree_node *x, int i)
{
	if (i >= x->key_count || i < 0) return;
	
	clear_key_position(x,i);
	
	if (i < x->key_count -1) 
	{
		// If we're not on the edge, shift everything over
		shift_bptree_node_elements_left(x, i+1);
		clear_key_position(x,x->key_count-1);
		
	}
	if(!x->leaf) 
	{
		shift_bptree_node_children_left(x,i+2);
		uuid_clear(x->children[i+1]);
	}
	x->key_count--;
}

 /*@ - Make copy and alloc new space 
  */
void copy_bptree_node_element(bptree_node *s, bptree_node *d,
		int s_pos, int d_pos)
{
	d->key_sizes[d_pos] = s->key_sizes[s_pos];
	d->value_sizes[d_pos] = s->value_sizes[s_pos];
	d->active[d_pos] = s->active[s_pos];
	
	d->keys[d_pos] = malloc(s->key_sizes[s_pos]);
	memcpy(d->keys[d_pos], s->keys[s_pos], s->key_sizes[s_pos]);
	d->values[d_pos] = malloc(s->value_sizes[s_pos]);
	memcpy(d->values[d_pos], s->values[s_pos], s->value_sizes[s_pos]);
}

/*@ Move btree element from one node to another; assumes there is space
 * Moves only references; clear pointers in original node 
 */
void move_bptree_node_element(bptree_node *s, bptree_node *d,
		int s_pos, int d_pos)
{
	d->key_sizes[d_pos] = s->key_sizes[s_pos];
	d->value_sizes[d_pos] = s->value_sizes[s_pos];
	d->active[d_pos] = s->active[s_pos];
	
	d->keys[d_pos] = s->keys[s_pos];
	d->values[d_pos] = s->values[s_pos];
	s->keys[s_pos] = NULL;
	s->values[s_pos] = NULL;
	s->key_sizes[s_pos] = -1;
	s->value_sizes[s_pos] = -1;
	s->active[s_pos] = 0;
}

/*@ Copies whatever is in x[j] n positions over; can be negative */
void copy_node_data(bptree_node *x, int j, int n)
{
	// Crash rather than do something stupid. We are proud, after all.
	assert(j+n < BPTREE_NODE_SIZE);
	assert(j+n >= 0);
	x->keys[j+n] = malloc(x->key_sizes[j]);
	memcpy(x->keys[j+n], x->keys[j], x->key_sizes[j]);
	x->key_sizes[j+n] = x->key_sizes[j];
	x->values[j+n] = malloc(x->value_sizes[j]);
	memcpy(x->values[j+n], x->values[j], x->value_sizes[j]);
	x->value_sizes[j+n] = x->value_sizes[j];
	x->active[j+n] = x->active[j];
}



void redistribute_keys_lr_nonleaf(bptree_node *p, 
				  bptree_node *cl, bptree_node *cr, int i)
{
	int l = cl->key_count - 1;
	while (cl->key_count - cr->key_count > 0)
	{
		shift_bptree_node_elements_right(cr, 0);
		shift_bptree_node_children_right(cr, 0);
		
		copy_bptree_node_element(p,cr,i,0);
		copy_bptree_node_element(cl,p,l,i);
		uuid_copy(cr->children[0], cl->children[l+1]);
		
		clear_key_position(cl, l);
		uuid_clear(cl->children[l+1]);
		
		cl->key_count--;
		cr->key_count++;
		l--;
	}
}

/*
 * In the right-to-left case for non-leaf children:
 *          | C |
 *  | A |          | D | E | F |
 *
 *   becomes:
 *           | D |
 * | A | C |        | E | F |
 * 
 * and equivalently for left-to-right
 */
void redistribute_keys_rl_nonleaf(bptree_node *p, 
				  bptree_node *cl, bptree_node *cr, int i)
{
	int l = cl->key_count;
	while (cr->key_count - cl->key_count > 0)
	{
		copy_bptree_node_element(p,cl,i,l);
		copy_bptree_node_element(cr,p,0,i);
		uuid_copy(cl->children[l+1],cr->children[0]);
		
		shift_bptree_node_elements_left(cr, 1);
		shift_bptree_node_children_left(cr, 1);
		
		clear_key_position(cr, cr->key_count-1);
		uuid_clear(cr->children[cr->key_count]);
		
		cr->key_count--;
		cl->key_count++;
		l++;
	}
}

void redistribute_keys_lr_leaf(bptree_node *p, 
			       bptree_node *cl, bptree_node *cr, int i)
{
	int j = cl->key_count -1;
	while (cl->key_count - cr->key_count > 0)
	{
		shift_bptree_node_elements_right(cr, 0);
		move_bptree_node_element(cl, cr, j, 0);
		clear_key_position(cl, cl->key_count-1);
		cl->key_count--;
		cr->key_count++;
		j--;
	}
	copy_bptree_node_element(cr, p, 0, i);
}

void redistribute_keys_rl_leaf(bptree_node *p, 
			       bptree_node *cl, bptree_node *cr, int i)
{
	int j = cl->key_count;
	while (cr->key_count - cl->key_count > 0)
	{
		copy_bptree_node_element(cr, cl, 0, j);
		shift_bptree_node_elements_left(cr, 1);
		clear_key_position(cr, cr->key_count-1);
		cr->key_count--;
		cl->key_count++;
		j++;
	}
	
	copy_bptree_node_element(cr, p, 0, i);
}

/*@ Merge keys from two child nodes into one */
void concatenate_nodes(bptree_node *p, bptree_node *cl, bptree_node *cr, int i)
{
	// Basic idea: Move splitting key from parent to end of left-side node
	// and move elements from right side after. 
	int r, l;
	
	if(!cl->leaf)
	{
		// If not concat'ing a leave we need to move down the split key 
		// because it won't be in the right leaf node
		move_bptree_node_element(p, cl, i, cl->key_count);
		cl->key_count++;
		uuid_copy(cl->children[cl->key_count], cr->children[0]);
		
	}
	else 
	{
		// Remap the linked-list along the leaves
		uuid_copy(cl->next_node, cr->next_node);
	}
	
	l = cl->key_count;
	int to_move = cr->key_count;
	for (r = 0; r < to_move; r++)
	{
		move_bptree_node_element(cr, cl, r, l);
		cl->key_count++;
		cr->key_count--;
		if(!cl->leaf) uuid_copy(cl->children[l+1], cr->children[r+1]);
		l++;
	}
	// 'delete' the old parent key by shifting everything over
	shift_bptree_node_elements_left(p,i+1);
	shift_bptree_node_children_left(p,i+2);
	clear_key_position(p,p->key_count-1);
	uuid_clear(p->children[p->key_count]);
	p->key_count--;
	if (p->key_count == 0) p->node_active = false; // A former root
	cr->node_active = 0; // "delete" the node
	
}


/*@ Evenly redistribute the keys in c1 and c2 to reduce the chances of underflow
 * in future deletes. 
 * See "Organization and maintenance of large ordered indices", Bayer, R., '72 */
void redistribute_keys(bptree_node *p, bptree_node *cl, bptree_node *cr, int i)
{
	/* There are four cases to handle:
	 * - Movement from left to right, right to left
	 * - For the above, when the child is a leaf or non-leaf
	 */
	assert(cl->leaf == cr->leaf && cr->key_count != cl->key_count);
	if (cl->leaf && cr->leaf)
	{
		if (cl->key_count < cr->key_count)
		{
			redistribute_keys_rl_leaf(p,cl,cr,i);
		}
		else
		{
			redistribute_keys_lr_leaf(p,cl,cr,i);
		}
	}
	else if (!cl->leaf && !cr->leaf)
	{
		if (cl->key_count < cr->key_count)
		{
			redistribute_keys_rl_nonleaf(p,cl,cr,i);
		}
		else
		{
			redistribute_keys_lr_nonleaf(p,cl,cr,i);
		}
	}
}

// As we use the Cormen definition, min size is degree - 1
inline int is_bptree_node_underflowed(bptree_node *x) 
{
	return x->key_count < BPTREE_NODE_MIN_SIZE;
}

bptree_node * bpnode_new()
{
	int i;
	bptree_node *n = malloc(sizeof(bptree_node));
	if (n == NULL) return NULL;
	memset(n, 0, sizeof(bptree_node));
	//n->key_count = 0;
	n->leaf = 1;
	n->node_active = 1;

	uuid_generate(n->self_key);
	//uuid_clear(n->next_node);
	//uuid_clear(n->prev_node);
	//uuid_clear(n->parent);
	for (i = 0; i < BPTREE_NODE_SIZE; i++)
	{
		n->key_sizes[i] = -1;
		n->value_sizes[i] = -1;
		//n->keys[i] = NULL;
		//n->values[i] = NULL;
		//n->active[i] = 0;
		//uuid_clear(n->children[i]);
	}
	//uuid_clear(n->children[BPTREE_NODE_SIZE]);

	return n;

}

/** Free all memory associated with node */
int free_node(bptree_node **n)
{
	if (*n == NULL) {
		int you_done_something_wrong = 1;
		assert(you_done_something_wrong == 0xDEADBEEF);
	}
	int i;
	for (i = 0; i < (*n)->key_count; i++)
	{
		free((*n)->keys[i]);
		free((*n)->values[i]);
		(*n)->keys[i] = NULL;
		(*n)->values[i] = NULL;
	}
	free(*n);
	*n = NULL;
	return BPTREE_OP_SUCCESS;
}

// Pack all non-dynamic array stuff first, in the order of the struct def
void * marshall_bptree_node(bptree_node *n, size_t *bsize)
{
	int _rv = is_node_sane(n);
	assert(_rv == 0);

	int i;
    /* creates buffer and serializer instance. */
    msgpack_sbuffer *buffer = msgpack_sbuffer_new();
    msgpack_packer *pck = msgpack_packer_new(buffer, msgpack_sbuffer_write);
    msgpack_sbuffer_clear(buffer);

    msgpack_pack_raw(pck, sizeof(uuid_t));
    msgpack_pack_raw_body(pck, n->self_key, sizeof(uuid_t));
    msgpack_pack_int16(pck, n->key_count);
    msgpack_pack_int16(pck, n->leaf);
    msgpack_pack_raw(pck, sizeof(uuid_t));
    msgpack_pack_raw_body(pck, n->parent, sizeof(uuid_t));
    msgpack_pack_raw(pck, sizeof(uuid_t));
    msgpack_pack_raw_body(pck, n->prev_node, sizeof(uuid_t));
    msgpack_pack_raw(pck, sizeof(uuid_t));
    msgpack_pack_raw_body(pck, n->next_node, sizeof(uuid_t));
    msgpack_pack_int16(pck, n->node_active);
    msgpack_pack_raw(pck, BPTREE_NODE_SIZE);
    msgpack_pack_raw_body(pck, n->active, BPTREE_NODE_SIZE);

    // Only pack data that is relevant based on the # of keys in this node

	for (i = 0; i < n->key_count; i++)
	{
		msgpack_pack_int32(pck, n->key_sizes[i]);
	}
	for (i = 0; i < n->key_count; i++)
	{
		msgpack_pack_int32(pck, n->value_sizes[i]);
	}
	for (i = 0; i < n->key_count; i++)
	{
		msgpack_pack_raw(pck, n->key_sizes[i]);
		msgpack_pack_raw_body(pck, n->keys[i], n->key_sizes[i]);
	}
	for (i = 0; i < n->key_count; i++)
	{
		msgpack_pack_raw(pck, n->value_sizes[i]);
		msgpack_pack_raw_body(pck, n->values[i], n->value_sizes[i]);
	}
	if (!n->leaf)
		for (i = 0; i <= n->key_count; i++) // Only write if non-leaf!
		{
			msgpack_pack_raw(pck, sizeof(uuid_t));
			msgpack_pack_raw_body(pck, n->children[i], sizeof(uuid_t));
		}

	// Don't free the sbuffer completely, cause we'll pass the data directly to
	// caller
//    msgpack_sbuffer_free(buffer);
    msgpack_packer_free(pck);
    *bsize = buffer->size;
    void *b = buffer->data;
    free(buffer);
	return b;
}


bptree_node * unmarshall_bptree_node(const void *buf,
		size_t sz, size_t *nsize)
{
    msgpack_zone z;
    msgpack_zone_init(&z, 4096);
    msgpack_object obj;
    msgpack_unpack_return ret;

	int i;
	size_t offset = 0;
	bptree_node *n = bpnode_new();
	*nsize = sizeof(bptree_node);

    ret = msgpack_unpack(buf, sz, &offset,&z, &obj);
    memcpy(n->self_key, obj.via.raw.ptr, sizeof(uuid_t));
    ret = msgpack_unpack(buf, sz, &offset,&z, &obj);
    n->key_count =  (int16_t) obj.via.i64;
    ret = msgpack_unpack(buf, sz, &offset,&z, &obj);
    n->leaf =  (int16_t) obj.via.i64;
    ret = msgpack_unpack(buf, sz, &offset,&z, &obj);
    memcpy(n->parent, obj.via.raw.ptr, sizeof(uuid_t));
    ret = msgpack_unpack(buf, sz, &offset,&z, &obj);
    memcpy(n->prev_node, obj.via.raw.ptr, sizeof(uuid_t));
    ret = msgpack_unpack(buf, sz, &offset,&z, &obj);
    memcpy(n->next_node, obj.via.raw.ptr, sizeof(uuid_t));
    ret = msgpack_unpack(buf, sz, &offset,&z, &obj);
    n->node_active = (int16_t) obj.via.i64;
    ret = msgpack_unpack(buf, sz, &offset,&z, &obj);
    memcpy(n->active, obj.via.raw.ptr, BPTREE_NODE_SIZE);

    // Only unpack data that is relevant based on the # of keys in this node

	for (i = 0; i < n->key_count; i++)
	{
		ret = msgpack_unpack(buf, sz, &offset,&z, &obj);
		n->key_sizes[i] = (int32_t) obj.via.i64;
	}
	for (i = 0; i < n->key_count; i++)
	{
		ret = msgpack_unpack(buf, sz, &offset,&z, &obj);
		n->value_sizes[i] = (int32_t) obj.via.i64;
	}
	for (i = 0; i < n->key_count; i++)
	{
		ret = msgpack_unpack(buf, sz, &offset,&z, &obj);
		*nsize+= obj.via.raw.size;
		n->keys[i] = malloc(obj.via.raw.size);
		memcpy(n->keys[i], obj.via.raw.ptr, obj.via.raw.size);
	}
	for (i = 0; i < n->key_count; i++)
	{
		ret = msgpack_unpack(buf, sz, &offset,&z, &obj);
		*nsize+= obj.via.raw.size;
		n->values[i] = malloc(obj.via.raw.size);
		memcpy(n->values[i], obj.via.raw.ptr, obj.via.raw.size);
	}
	if (!n->leaf)
		for (i = 0; i <= n->key_count; i++)
		{
			ret = msgpack_unpack(buf, sz, &offset,&z, &obj);
			memcpy(n->children[i], obj.via.raw.ptr, sizeof(uuid_t));
		}
	assert(ret == MSGPACK_UNPACK_SUCCESS);
	msgpack_zone_destroy(&z);
	return n;
}



/*@ Return 0 if node is good; < 0 for a variety of issues with the node */
int is_node_sane(bptree_node *n)
{
	int i =0;
	// Do some sanity checking of the node; these two checks should
	// probably be enough to catch most cases of corrupt or uninitialized nodes
	if (!n->node_active) return 0; // Skip these checks if node is deleted
	if (n == NULL) return -1;
	if (n->key_count < 0 || n->key_count > BPTREE_NODE_SIZE) return -2;
	// Ensure we don't have any weird corner cases with the children of node
	if (!n->leaf && n->key_count > 0)
		if (uuid_is_null(n->children[n->key_count]) ||
				uuid_is_null(n->children[0])) return -3;

	for (i = 0; i < n->key_count; i++)
	{
		if(n->keys[i] == NULL) return -4;
		if(n->values[i] == NULL) return -5;
		if(n->key_sizes[i] < 0) return -6;
		if(n->value_sizes[i] < 0) return -7;
	}

	if(!n->leaf && n->key_count > 0) {
		for (i = 0; i <= n->key_count; i++) {
			if(uuid_is_null(n->children[i])) return -8;
		}
	}

	// Make sure anything outside boundaries is not filled with anything
	for (i = n->key_count; i < BPTREE_NODE_SIZE; i++)
	{
		if(n->keys[i] != NULL || n->values[i] != NULL) return -9;
		if(n->key_sizes[i] != -1 || n->value_sizes[i] != -1) return -10;
		if(!uuid_is_null(n->children[i+1])) return -11;
	}
	if(n->key_count < BPTREE_NODE_SIZE &&
			!uuid_is_null(n->children[BPTREE_NODE_SIZE]) ) return -12;
	return 0;
}

int is_correct_node(bptree_node *n, uuid_t node_key)
{
	// Did we actually get the right node back?
	if (n == NULL || node_key == NULL) return 0;
	return uuid_compare(n->self_key, node_key) == 0;
}


/** Copy all memory associated with node and return a new pointer */
bptree_node *copy_node(bptree_node *n)
{
	assert(is_node_sane(n) == 0);
	bptree_node *x;
	//x = malloc(sizeof(bptree_node));
	// Ensure we use the standard way of creating a new , uninitialized node
	x = bpnode_new();
	int i;
	uuid_copy(x->self_key, n->self_key);
	x->key_count = n->key_count;
	x->leaf = n->leaf;
	uuid_copy(x->parent, n->parent);
	uuid_copy(x->prev_node, n->prev_node);
	uuid_copy(x->next_node, n->next_node);
	for (i = 0; i < n->key_count; i++)
	{
		x->key_sizes[i] = n->key_sizes[i];
		x->value_sizes[i] = n->value_sizes[i];
		x->keys[i] = malloc(x->key_sizes[i]);
		x->values[i] = malloc(x->value_sizes[i]);
		memcpy(x->keys[i], n->keys[i],n->key_sizes[i]);
		memcpy(x->values[i], n->values[i],n->value_sizes[i]);
		uuid_copy(x->children[i], n->children[i]);
		x->active[i] = n->active[i];

	}
	uuid_copy(x->children[n->key_count], n->children[n->key_count]);
//	x->write_count = n->write_count;
//	x->last_version = n->last_version;
	return x;
}


int bpnode_is_full(bptree_node* n)
{
	return (n->key_count == (BPTREE_NODE_SIZE));
}


void copy_key_val_to_node(bptree_node *x, bptree_key_val *kv, int pos)
{
	x->keys[pos] = malloc(kv->ksize);
	x->values[pos] = malloc(kv->vsize);
	memcpy(x->keys[pos], kv->k, kv->ksize);
	x->key_sizes[pos] = kv->ksize;
	memcpy(x->values[pos], kv->v, kv->vsize);
	x->value_sizes[pos] = kv->vsize;
	x->active[pos] = 1;
	x->key_count++;
}


// Allocates memory for and copies contents of kv1 into kv2
void copy_key_val(bptree_key_val *dest, bptree_key_val *src)
{
	assert(src->ksize < BPTREE_MAX_VALUE_SIZE);
	assert(src->vsize < BPTREE_MAX_VALUE_SIZE);
	dest->k = malloc(src->ksize);
	dest->v = malloc(src->vsize);
	memcpy(dest->k, src->k, src->ksize);
	memcpy(dest->v, src->v, src->vsize);
	dest->ksize = src->ksize;
	dest->vsize = src->vsize;
}
bptree_key_val * bpnode_get_kv(bptree_node *n, int i)
{
	bptree_key_val *kv = malloc(sizeof(bptree_key_val));
	assert(n->key_sizes[i] < BPTREE_MAX_VALUE_SIZE);
	assert(n->value_sizes[i] < BPTREE_MAX_VALUE_SIZE);
	kv->k = malloc(n->key_sizes[i]);
	kv->v = malloc(n->value_sizes[i]);
	memcpy(kv->k, n->keys[i], n->key_sizes[i]);
	memcpy(kv->v, n->values[i], n->value_sizes[i]);
	kv->ksize = n->key_sizes[i];
	kv->vsize = n->value_sizes[i];
	return kv;
}
void bpnode_get_kv_ref(bptree_node *n, int i, bptree_key_val *kv)
{
	kv->k = n->keys[i];
	kv->v = n->values[i];
	kv->ksize = n->key_sizes[i];
	kv->vsize = n->value_sizes[i];
}

void bpnode_pop_kv(bptree_node *n, int i, bptree_key_val *kv)
{
	memcpy(kv->k, n->keys[i], n->key_sizes[i]);
	memcpy(kv->v, n->values[i], n->value_sizes[i]);
	kv->ksize = n->key_sizes[i];
	kv->vsize = n->value_sizes[i];
}
