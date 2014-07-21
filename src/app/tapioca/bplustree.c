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

/**

 * Adapted from integer-based internal btree from tapioca sources
 *
 * This is a b+tree implementation that operates external to the tapioca
 * cluster and uses the basic get/put/commit API to provide b+tree functionality
 * Arbitrary binary data can be indexed provided that a pointer to a function
 * is provided for ordering (similar to qsort())
 */
#include "bplustree.h"
#include "tcp.h"

int file_opened =0;
FILE *trace_fp;

static char uudbg[40];

static int bptree_search_recursive(bptree_session *bps,
		bptree_node* n, bptree_key_val *kv);
static int bptree_insert_nonfull(bptree_session *bps,
		bptree_node* x, bptree_key_val *kv, int lvl);
static int bptree_update_recursive(bptree_session *bps,
		bptree_node* x,bptree_key_val *kv);
static int bptree_index_first_recursive(
		bptree_session *bps, void *k, int32_t *ksize, void *v, int32_t *vsize,
		bptree_node *n);
static int 
bptree_delete_recursive(bptree_session *bps, bptree_node* x, bptree_key_val *kv);
static int bptree_split_child(bptree_session *bps,
		bptree_node* x, int i, bptree_node* y, int lvl);
static int node_is_full(bptree_node * n);
//inline
void * create_cell_key(bptree_session *bps, uuid_t cell_id, size_t *ksize);
void * create_meta_key(bptree_session *bps, size_t *ksize);
static void get_unique_key(bptree_session *bps, uuid_t uu);
bptree_meta_node * create_bptree_or_reset(bptree_session *bps, int *rv);
bptree_node * copy_node(bptree_node *n);
int free_node(bptree_node **n);
void free_meta_node(bptree_meta_node **m);
int bptree_index_next_internal(bptree_session *bps, void *k,
		int32_t *ksize, void *v, int32_t *vsize);


int is_valid_traversal(bptree_session *bps, bptree_node *x,
		bptree_node *n,int i);
bptree_node * unmarshall_bptree_node_msgpack(const void *buf, size_t sz,
		size_t *nsize);
inline int num_fields_used(bptree_session *bps, const bptree_key_val *kv) ;
void clear_key_position(bptree_node *x, int i);

/* There is no longer any good reason to have commit done inside of
* these methods; the client is unlikely to want to have a committed  bpt 
* if the other metadata it writes has to be retried. i.e. merge these next 
* two methods together or rename this one
* A special open-only form of the init bpt session that does not call commit
* REQUIRES an externally-maintained UNIQUE execution_id to be provided!! */
int bptree_initialize_bpt_session_no_commit(bptree_session *bps,
		tapioca_bptree_id bpt_id, enum bptree_open_flags open_flags,
		enum bptree_insert_flags insert_flags,
		uint32_t local_execution_id)
{
	int rv;
	if (open_flags != BPTREE_OPEN_ONLY) return BPTREE_OP_INVALID_INPUT;
	bptree_meta_node *bpm;
	bptree_set_active_bpt_id(bps, bpt_id);
	bps->op_counter = 1;
	bps->idx_len = 0;
	bps->insert_flags = insert_flags;
	bpm = read_meta_node(bps,&rv);
	if (rv != BPTREE_OP_NODE_FOUND) return rv;
	assert(bpm->bpt_id == bpt_id);
	bps->execution_id = local_execution_id;

	printf("No-commit connection to b+tree id %d client id %d exec %d\n",
			bpt_id, bps->tapioca_client_id, bps->execution_id);
	fflush(stdout);
#ifdef DISABLED_______DEFENSIVE_MODE
	bptree_node *root;
	root = read_node(bps, bpm->root_key, &rv);
	if(rv != BPTREE_OP_NODE_FOUND) return rv;
	assert(uuid_compare(bpm->root_key, root->self_key) == 0);
	assert(uuid_is_null(root->parent));
#endif
	return BPTREE_OP_SUCCESS;
}

int bptree_initialize_bpt_session(bptree_session *bps,
		tapioca_bptree_id bpt_id, enum bptree_open_flags open_flags,
		enum bptree_insert_flags insert_flags)
{
	int rv, rv_c,attempts;
	bptree_meta_node *bpm;

	bptree_set_active_bpt_id(bps, bpt_id);
	bps->op_counter = 1;
	bps->idx_len = 0;
	bps->insert_flags = insert_flags;
	rv_c = -1;
	attempts = 0;
	bps->cached_key_dirty = 1; // Expire anything that may be cached
	
	switch (open_flags) {
		case BPTREE_OPEN_ONLY:
		case BPTREE_OPEN_CREATE_IF_NOT_EXISTS:

			// Do read stuff here
			bpm = read_meta_node(bps,&rv);

			if (rv == BPTREE_OP_TAPIOCA_NOT_READY ||
					rv == BPTREE_OP_METADATA_ERROR) return rv;

			if (open_flags == BPTREE_OPEN_ONLY ||
					rv == BPTREE_OP_NODE_FOUND) break;

			// Yes, the lack of break here is intentional
		case BPTREE_OPEN_OVERWRITE:
			// If we're here, we're overwriting or we're creating a bpt
			// that didn't exist
			bpm = create_bptree_or_reset(bps, &rv);
			if (rv != BPTREE_OP_NODE_FOUND) return rv;
			break;
		default:
			return BPTREE_OP_INVALID_INPUT;
			break;
	}
	assert(bpm->bpt_id == bpt_id);
	bps->execution_id = ++bpm->execution_id;
	write_meta_node(bps, bpm, NULL);
	free_meta_node(&bpm);
	printf( "Connected to b+tree id %d node id %d exec %d %d attempts "
			"rv_c: %d \n", bpt_id, bps->tapioca_client_id,
			bps->execution_id, attempts, rv_c);
	fflush(stdout);

	return BPTREE_OP_SUCCESS;
}

int bptree_set_active_bpt_id(bptree_session *bps, tapioca_bptree_id bpt_id)
{
	bps->bpt_id = bpt_id;
	bps->eof = 0;
	bps->cursor_pos = 0;
//	uuid_clear(bps->cursor_cell_id);
	bps->cursor_node = NULL;
	return BPTREE_OP_SUCCESS;
}

int bptree_set_num_fields(bptree_session *bps, int16_t num_fields)
{
	if (num_fields > BPTREE_MAX_NUMBER_FIELDS) return BPTREE_OP_INVALID_INPUT;
	bps->bfield = malloc(sizeof(bptree_field) * num_fields);
	bps->num_fields = num_fields;
	return BPTREE_OP_SUCCESS;
}

int bptree_set_field_info(bptree_session *bps, int16_t field_num,
		int16_t field_sz, enum bptree_field_comparator field_type,
		int(*compar)(const void *, const void *, size_t n))
{

	bps->bfield[field_num].f_sz = field_sz;
	bps->bfield[field_num].compar = compar;
	bps->bfield[field_num].field_type = field_type;
	bps->idx_len += field_sz;

	return BPTREE_OP_SUCCESS;

}

bptree_node * create_new_empty_bptree_node()
{
	int i;
	bptree_node *n = malloc(sizeof(bptree_node));
	if (n == NULL) return NULL;
	n->key_count = 0;
	n->leaf = 1;
	n->node_active = 1;

	uuid_clear(n->self_key);
	uuid_clear(n->next_node);
	uuid_clear(n->prev_node);
	uuid_clear(n->parent);
	for (i = 0; i < BPTREE_NODE_SIZE; i++)
	{
		n->key_sizes[i] = -1;
		n->value_sizes[i] = -1;
		n->keys[i] = NULL;
		n->values[i] = NULL;
		n->active[i] = 0;
		uuid_clear(n->children[i]);
	}
	uuid_clear(n->children[BPTREE_NODE_SIZE]);

	return n;

}
bptree_node * create_new_bptree_node(bptree_session *bps)
{
	bptree_node *n = create_new_empty_bptree_node();
	get_unique_key(bps, n->self_key);
	return n;
}

/** Reset/create contents of bptree handle; will need to be commited after
 * Returns:
 * BPTREE_OP_METADATA_ERROR - if problem writing root or meta node
 * From read_meta_node:
 * BPTREE_OP_NODE_FOUND -- On success
 * BPTREE_OP_TAPIOCA_NOT_READY - if tapioca didn't grab the key yet
 * BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT - metanode didn't exist or corrupt
 */
bptree_meta_node * create_bptree_or_reset(bptree_session *bps, int *rv)
{
	bptree_node *root;
	bps->execution_id = 1;
	bps->op_counter = 0;

	root = create_new_bptree_node(bps);

	bptree_meta_node bpm;
	bpm.bpt_id = bps->bpt_id;
	bpm.execution_id = 1;
	uuid_copy(bpm.root_key, root->self_key);

	if (write_meta_node(bps, &bpm, root) < 0)
	{
		*rv = BPTREE_OP_METADATA_ERROR;
		return NULL;
	}
	free(root);
	return read_meta_node(bps, rv);

}

int bptree_search(bptree_session *bps, void *k,
		int32_t ksize, void *v, int32_t *vsize)
{
	int rv;
	bptree_node *root;
	bptree_meta_node *bpm;
	bptree_key_val kv;
	kv.k = k;
	kv.v = v;
	kv.ksize = ksize;

	if (ksize <= 0 || ksize >= 65000) return BPTREE_OP_INVALID_INPUT;
	
	rv = bptree_read_root(bps, &bpm, &root);
	if (rv != BPTREE_OP_NODE_FOUND) return rv;

	rv = bptree_search_recursive(bps, root, &kv);
	if (rv != BPTREE_OP_KEY_FOUND)
	{
		*vsize = 0;
	}
	*vsize = kv.vsize;

	free_node(&root);
	free_meta_node(&bpm);
	return rv;
}

/*@
 * Delete is basically the same as update, but sets the key found to inactive
 * Return 0 if the key was not found (and do nothing), -1 on error
 * and 1 on success
 */
int bptree_delete(bptree_session *bps, void *k,
		int32_t ksize, void *v, int32_t vsize)
{
	int rv;

	bptree_node *root;
	bptree_meta_node *bpm;
	bptree_key_val kv;
	kv.k = k;
	kv.v = v;
	kv.ksize = ksize;
	kv.vsize = vsize;

	rv = bptree_read_root(bps, &bpm, &root);
	if (rv != BPTREE_OP_NODE_FOUND) return rv;

	rv = bptree_delete_recursive(bps, root, &kv);
	
	if (root->key_count == 0) {
		// Set new root
		uuid_copy(bpm->root_key, root->children[0]);
		uuid_clear(root->parent);
		if (write_meta_node(bps, bpm, NULL) < 0) 
			return BPTREE_OP_METADATA_ERROR;
	}

	free_node(&root);
	free_meta_node(&bpm);
	return rv;
}

// As we use the Cormen definition, min size is degree - 1
inline int is_bptree_node_underflowed(bptree_node *x) 
{
	return x->key_count < BPTREE_NODE_MIN_SIZE;
}

static int 
bptree_delete_recursive(bptree_session *bps, bptree_node* x, bptree_key_val *kv)
{
	int i,rv;

#ifdef DEFENSIVE_MODE
	assert(is_cell_ordered(bps,x));
	assert(are_key_and_value_sizes_valid(x));
#endif

	i = find_position_in_node(bps, x, kv, &rv);
	
	if (x->leaf)
	{
		if (rv == BPTREE_OP_KEY_FOUND)
		{
			delete_key_from_node(x,i);
			if (write_node(bps, x) != BPTREE_OP_SUCCESS) 
				return BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT;
		}
		return rv;
	}
	else
	{
		bptree_node *c;

		if (rv == BPTREE_OP_KEY_FOUND)
		{
			x->active[i] = 0; 
			if (write_node(bps, x) != BPTREE_OP_SUCCESS)
				return BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT;
		}
		c = read_node(bps, x->children[i], &rv);
		if (rv != BPTREE_OP_NODE_FOUND) return rv;
		
		rv = bptree_delete_recursive(bps, c, kv);
		
		// Check if we have an underflow condition
		if(is_bptree_node_underflowed(c))
		{
			rebalance_nodes(bps, x, c, i);
		}
		
		free_node(&c);
		return rv;
	}
}

/*@
 * Given parent node *p and child *c in position i that has now underflowed, 
 * merge *c with an adjacent node
 */
int rebalance_nodes(bptree_session *bps, bptree_node *p, 
			       bptree_node *c, int i)
{
	int i_adj, rv;
	bptree_node *adj, *cl, *cr;
	// Choose an adjacent node to redistribute with
	i_adj = i+1;
	if (i >= p->key_count -1) {
		i_adj = i-1;
	}
	cl = c;
	cr = adj = read_node(bps, p->children[i_adj], &rv);
	if (rv != BPTREE_OP_NODE_FOUND) return rv;
	assert(!is_bptree_node_underflowed(adj));
	
	if (i_adj < i)
	{
		cl = adj;
		cr = c;
	}
	
	if (c->key_count + adj->key_count < BPTREE_NODE_SIZE)
	{
		// Concatenate (i.e. remove one) nodes
		concatenate_nodes(bps, p, cl, cr, i);
	} 
	else 
	{
		// Redistribute keys among nodes
		redistribute_keys(bps, p, cl, cr, i);
	}
	// cl is about to become the new root, clear parent before writing back
	if (p->key_count == 0) {
		uuid_clear(cl->parent);
	}
	
	if (write_node(bps, p) != BPTREE_OP_SUCCESS)
		return BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT;
	if (write_node(bps, cl) != BPTREE_OP_SUCCESS)
		return BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT;
	if (write_node(bps, cr) != BPTREE_OP_SUCCESS)
		return BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT;
	return BPTREE_OP_SUCCESS;
}

/*@ Evenly redistribute the keys in c1 and c2 to reduce the chances of underflow
 * in future deletes. 
 * See "Organization and maintenance of large ordered indices", Bayer, R., '72 */
int redistribute_keys(bptree_session *bps, bptree_node *p, bptree_node *cl,
		      bptree_node *cr, int i)
{
	int j;
	// Don't remove any nodes, but rearrange keys in nodes from left and 
	// right to eliminate the underflow
	if (cl->key_count < cr->key_count)
	{
		j = cl->key_count;
		// Flow right to left
		while (cr->key_count - cl->key_count > 0)
		{
			move_bptree_node_element(cr, cl, 0, j, true);
			// TODO Move the node keycount maintenance logic into fn
			cr->key_count--;
			cl->key_count++;
			shift_bptree_node_elements_left(cr, 1);
			if(!cl->leaf) shift_bptree_node_children_left(cr, 1);
			j++;
		}
	}
	else
	{
		// Flow left to right
		j = cl->key_count -1;
		while (cl->key_count - cr->key_count > 0)
		{
			shift_bptree_node_elements_right(cr, 0);
			if(!cr->leaf) shift_bptree_node_children_right(cr,0);
			move_bptree_node_element(cl, cr, j, 0, true);
			// TODO Move the node keycount maintenance logic into fn
			j--;
		}
	}
	// After we're done moving elements across, copy the
	// new left-most element in *cr to the parent node
	move_bptree_node_element(cr, p, 0, i, false);
	
}

/*@ Merge keys from two child nodes into one */
int concatenate_nodes(bptree_session *bps, bptree_node *p, bptree_node *cl,
		      bptree_node *cr, int i)
{
	// Basic idea: Move splitting key from parent to end of left-side node
	// and move elements from right side after. 
	int r, l;
	
	if(!cl->leaf)
	{
		// If not concat'ing a leave we need to move down the split key 
		move_bptree_node_element(p, cl, cl->key_count, i, false);
	}
	
	l = cl->key_count;
	int to_move = cr->key_count;
	for (r = 0; r < to_move; r++)
	{
		move_bptree_node_element(cr, cl, r, l, false);
		if(!cl->leaf) shift_bptree_node_children_left(cr, 1);
		// Move children properly
		l++;
	}
	// 'delete' the old parent key by shifting everything over
	shift_bptree_node_elements_left(p,i+1);
	shift_bptree_node_children_left(p,i+1);
	clear_key_position(p,p->key_count-1);
	uuid_clear(p->children[p->key_count]);
	p->key_count--;
	if (p->key_count == 0) p->node_active = false; // A former root
	is_node_sane(p);
	is_node_sane(cl);
	cr->node_active = 0; // "delete" the node
	
}

/*@
 * Update functions are basically simplified versions of ones for insert();
 * Return 0 if the key was not found (and do nothing), -1 on error
 * and 1 on success
 */
int bptree_update(bptree_session *bps, void *k,
		int32_t ksize, void *v, int32_t vsize)
{
	int rv;

	bptree_node *root;
	bptree_meta_node *bpm;
	bptree_key_val kv;
	kv.k = k;
	kv.v = v;
	kv.ksize = ksize;
	kv.vsize = vsize;

	if (ksize <= 0 || ksize >= 65000
		|| vsize <= 0 || vsize >= 65000) return BPTREE_OP_INVALID_INPUT;

	rv = bptree_read_root(bps, &bpm, &root);
	if (rv != BPTREE_OP_NODE_FOUND) return rv;

	rv = bptree_update_recursive(bps, root, &kv);

	free_node(&root);
	free_meta_node(&bpm);
	return rv;
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
bptree_key_val * copy_key_val_from_node(bptree_node *n, int i)
{
	bptree_key_val *kv = malloc(sizeof(bptree_key_val));
	assert(n->key_sizes[i] < BPTREE_MAX_VALUE_SIZE);
	assert(n->value_sizes[i] < BPTREE_MAX_VALUE_SIZE);
	kv->k = malloc(n->key_sizes[i]);
	kv->v = malloc(n->value_sizes[i]);
	get_key_val_from_node(n, i, kv);
	return kv;
}
// Another utility method to eliminate a lot of messy code
inline void get_key_val_from_node(bptree_node *n, int i, bptree_key_val *kv)
{
	kv->k = n->keys[i];
	kv->v = n->values[i];
	kv->ksize = n->key_sizes[i];
	kv->vsize = n->value_sizes[i];
}

inline void free_key_val(bptree_key_val **kv)
{
	if(*kv == NULL) assert (0 == 0xDEADBEEF);
	free((*kv)->k);
	free((*kv)->v);
	(*kv)->k = NULL;
	(*kv)->v = NULL;
	free(*kv);
	*kv = NULL;
}

// Wrapper for main compar() method but using key/val structs
inline int bptree_compar_keys(bptree_session *bps,
		const bptree_key_val *kv1, const bptree_key_val *kv2)
{
	return bptree_compar(bps, kv1->k, kv2->k, kv1->v, kv2->v,
			kv1->vsize,kv2->vsize, bps->num_fields);

}

inline int bptree_compar_to_node(bptree_session *bps,
	bptree_node *x, const bptree_key_val *kv, int pos)
{
	char nb = '\0';
	int num_fields = num_fields_used(bps, kv);
	if (bps->insert_flags == BPTREE_INSERT_ALLOW_DUPES) {
		return bptree_compar(bps, x->keys[pos], kv->k, x->values[pos],
			kv->v, x->value_sizes[pos],kv->vsize, num_fields);
	}
	else 
	{
		return bptree_compar(bps, x->keys[pos], kv->k, &nb,&nb,1,1, num_fields);
	}
	

}

/*@ Calculate the number of fields actually used in the referenced kv */
inline int num_fields_used(bptree_session *bps, const bptree_key_val *kv) {
	int i, acc, fields = 0;
	acc = 0;
	bptree_field *bf = bps->bfield;
	for (i = 1; i <= bps->num_fields; i++) {
		acc += bf->f_sz;
		bf++;
		if (kv->ksize <= acc) return i;
	}
	
	return bps->num_fields;
	
}

// A generalization of the old compar function we used, but incorporating the
// information we have provided about what fields are present and their
// individual compar functions
// TODO In order for non-unique secondary keys to work, we have to also compare
// the values; this is a bit clunky now and probably should be rethought a bit
// FIXME There is a problem here; we need to STOP going through the fields
// if the key we are comparing against is only a partial key!
int bptree_compar(bptree_session *bps, const void *k1, const void *k2,
		const void *v1, const void *v2, size_t vsize1, size_t vsize2, 
		int tot_fields)
{
	int i, res, offset;
	bptree_field *bf = bps->bfield;
	const unsigned char *a1 = k1;
	const unsigned char *a2 = k2;
	offset = 0;
	//for (i = 1; i <= bps->num_fields; i++)
	for (i = 1; i <= tot_fields; i++)
	{
		const void *u = a1 + offset;
		const void *v = a2 + offset;
		res = (*bf->compar)(u, v, bf->f_sz);
		if (res != 0)
			return res;
		offset += bf->f_sz;
		bf++;
	}

	// Key is the same; return whether the values are the same
	if (vsize1 != vsize2) return (vsize1 < vsize2) ? -1 : 1;
	return memcmp(v1, v2, vsize1);
}

// TODO This method will only update ONE value for the given key, so it is
// only is appropriate for primary-key based updates
static int bptree_update_recursive(bptree_session *bps,
		bptree_node* x, bptree_key_val *kv)
{
	int i,rv ;

#ifdef DEFENSIVE_MODE
	assert(is_cell_ordered(bps,x));
	assert(are_key_and_value_sizes_valid(x));
#endif

	i = find_position_in_node(bps, x, kv, &rv);
	if (x->leaf)
	{
		if (rv == BPTREE_OP_KEY_NOT_FOUND)
		{
			// Key was not found where it should have been; do nothing
			return rv;
		}
		
		unsigned char *newval = realloc(x->values[i],kv->vsize);
		assert(newval != NULL); // just in case we can't reallocate...
		x->values[i] = newval;
		memcpy(x->values[i], kv->v, kv->vsize);
		x->value_sizes[i] = kv->vsize;
		return write_node(bps, x);
	}
	else
	{
		bptree_node *n = read_node(bps, x->children[i], &rv);
		if (rv != BPTREE_OP_NODE_FOUND) return rv;
		assert(is_valid_traversal(bps, x, n, i));

		if (i <= x->key_count && i > 0)
		{
			// FIXME Refactor out reference to bptree_compar
			if (bptree_compar_to_node(bps,x,kv, i-1) == 0)
			{
				unsigned char *newval = realloc(x->values[i-1],kv->vsize);
				assert(newval != NULL); // just in case we can't reallocate...
				x->values[i-1] = newval;
				memcpy(x->values[i-1], kv->v, kv->vsize);
				x->value_sizes[i-1] = kv->vsize;
				rv = write_node(bps, x);
				if (rv != BPTREE_OP_SUCCESS) return rv;
			}
		}
		rv = bptree_update_recursive(bps, n, kv);
		free_node(&n);
		return rv;
	}
}

int bptree_insert(bptree_session *bps, void *k, int ksize,
		void *v, int vsize)
{
	int rv;
	bptree_key_val kv;
	bptree_node *root;
	bptree_meta_node *bpm;
	unsigned char _val[65000];
	int _vsize;

	if (ksize <= 0 || ksize >= 65000
		|| vsize <= 0 || vsize >= 65000) return BPTREE_OP_INVALID_INPUT;
	
	rv = bptree_search(bps, k, ksize, _val, &_vsize);

	kv.k = k;
	kv.v = v;
	kv.ksize = ksize;
	kv.vsize = vsize;
	
	if (rv == BPTREE_OP_KEY_FOUND 
		&& bps->insert_flags == BPTREE_INSERT_UNIQUE_KEY)
		return BPTREE_ERR_DUPLICATE_KEY_INSERTED;

	if(! (rv == BPTREE_OP_KEY_NOT_FOUND ||
		rv == BPTREE_OP_KEY_FOUND)) return rv;

	rv = bptree_read_root(bps, &bpm, &root);
	if (rv != BPTREE_OP_NODE_FOUND) return rv;

	if (node_is_full(root))
	{
		bptree_node *newroot = create_new_bptree_node(bps);
		newroot->leaf = 0;
		uuid_copy(newroot->children[0], root->self_key);
		uuid_copy(bpm->root_key, newroot->self_key);
		uuid_copy(root->parent, newroot->self_key);

		if (write_meta_node(bps, bpm, newroot) < 0) return BPTREE_OP_METADATA_ERROR;
		rv = bptree_split_child(bps, newroot,0, root, 1);
		assert(rv == BPTREE_OP_SUCCESS);
		rv = bptree_insert_nonfull(bps, newroot, &kv, 1);

		assert(newroot->key_count > 0);
		assert(uuid_compare(newroot->self_key, root->parent) == 0);
		assert(uuid_compare(newroot->self_key, bpm->root_key) == 0);
		free_node(&newroot);
	}
	else
	{
		rv = bptree_insert_nonfull(bps, root, &kv, 1);
	}
	free_node(&root);
	free_meta_node(&bpm);
	//assert(rv != BPTREE_OP_TAPIOCA_NOT_READY);
	if (rv == BPTREE_OP_TAPIOCA_NOT_READY) 
	{
		// Since we search the whole insert path before, we should have this
		// situation arise; but if it does, we will have corrupted the
		// state of the tree
		return BPTREE_OP_RETRY_NEEDED;
		//return BPTREE_OP_TAPIOCA_NOT_READY;
	}
		
	assert(rv == BPTREE_OP_SUCCESS || rv == BPTREE_ERR_DUPLICATE_KEY_INSERTED);
	return rv;

}

static int bptree_split_child(bptree_session *bps,
		bptree_node* x, int i, bptree_node* y, int lvl)
{
	int j,rv;
#ifdef DEFENSIVE_MODE
	assert(is_node_sane(x));
	assert(is_node_sane(y));
	assert(is_cell_ordered(bps,x));
	assert(is_cell_ordered(bps,y));
#endif
#ifdef TRACE_MODE
	write_split_to_trace(bps, x, y, NULL, i, lvl);
#endif

	shift_bptree_node_children_right(x, i);
	shift_bptree_node_elements_right(x, i);
	
	// Move the split key up; if y is a leaf, copy, if not, move it
	move_bptree_node_element(y, x, BPTREE_MIN_DEGREE - 1, i, !y->leaf);

	bptree_node *n= create_new_bptree_node(bps);
	n->leaf = y->leaf;
	/*
	if (y->leaf)
	{
		// If y is a leaf, we want to make sure we maintain the median key
		n->key_count = BPTREE_MIN_DEGREE;
		y->key_count = BPTREE_MIN_DEGREE - 1;
	}
	else {
		// If we are splitting a non-leaf, we have BP_DEG * 2 children to
		// distribute while the median node goes up
		n->key_count = BPTREE_MIN_DEGREE - 1;
		y->key_count = BPTREE_MIN_DEGREE - 1;
	}
	*/
	// Shift over elements into new node and null out old stuff
	for (j = 0; j < BPTREE_NODE_MIN_SIZE + y->leaf; j++)
	{
		int shift = j + BPTREE_MIN_DEGREE - y->leaf;
		move_bptree_node_element(y, n, shift, j, true);
	}

	uuid_copy(x->children[i + 1], n->self_key);
	uuid_copy(n->parent, x->self_key);
	uuid_copy(y->parent, x->self_key);

	if (!y->leaf)
	{
		// Shift over children equally among the two
		for (j = 0; j < BPTREE_MIN_DEGREE; j++)
		{
			int shift = j + BPTREE_MIN_DEGREE;
			uuid_copy(n->children[j], y->children[shift]);
			uuid_clear(y->children[shift]);
		}
	}
	else
	{
		uuid_copy(n->next_node, y->next_node);
		uuid_copy(y->next_node, n->self_key);
		uuid_copy(n->prev_node, y->self_key);
	}


#ifdef DEFENSIVE_MODE
	assert(is_cell_ordered(bps, x));
	assert(is_cell_ordered(bps, y));
	assert(is_cell_ordered(bps, n));
	are_split_cells_valid(bps, x,i,y,n); // Assertions called inside
#endif

#ifdef TRACE_MODE
	write_split_to_trace(bps, x, y, new, i, lvl);
#endif

	if (write_node(bps, x) != BPTREE_OP_SUCCESS)
		return BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT;
	if (write_node(bps, y) != BPTREE_OP_SUCCESS)
		return BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT;
	if (write_node(bps, n) != BPTREE_OP_SUCCESS)
		return BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT;

	free_node(&n);
	return BPTREE_OP_SUCCESS;
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
/*@ Shift the elements of bptree_node right at position pos*/
void shift_bptree_node_elements_right(bptree_node *x, int pos)
{
	assert(x->key_count < BPTREE_NODE_SIZE);
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
	for (j = x->key_count ; j > pos; j--)
	{
		uuid_copy(x->children[j + 1],x->children[j]);
	}
}

/*@ Shift the elements of bptree_node right at position pos*/
void shift_bptree_node_elements_left(bptree_node *x, int pos)
{
	assert(x->key_count < BPTREE_NODE_SIZE && pos > 0);
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
	for (j = pos; j < x->key_count; j++)
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
		if(!x->leaf) shift_bptree_node_children_left(x,i+1);
		clear_key_position(x,x->key_count-1);
		
	}
	x->key_count--;
}

/*@ Move btree element from one node to another; assumes there is space
 * Modes available:
 * - Move only references; clear pointers in original node (move = 1)
 * - Make copy and alloc new space (move = 0)
 * */
void move_bptree_node_element(bptree_node *s, bptree_node *d,
		int s_pos, int d_pos, int move)
{
	d->key_sizes[d_pos] = s->key_sizes[s_pos];
	d->value_sizes[d_pos] = s->value_sizes[s_pos];
	d->active[d_pos] = s->active[s_pos];
	if (move)
	{
		d->keys[d_pos] = s->keys[s_pos];
		d->values[d_pos] = s->values[s_pos];
		s->keys[s_pos] = NULL;
		s->values[s_pos] = NULL;
		s->key_sizes[s_pos] = -1;
		s->value_sizes[s_pos] = -1;
		s->active[s_pos] = 0;
		s->key_count--;
	}
	else
	{
		d->keys[d_pos] = malloc(s->key_sizes[s_pos]);
		memcpy(d->keys[d_pos], s->keys[s_pos], s->key_sizes[s_pos]);
		d->values[d_pos] = malloc(s->value_sizes[s_pos]);
		memcpy(d->values[d_pos], s->values[s_pos], s->value_sizes[s_pos]);
	}
	d->key_count++;

}

/*
inline int is_key_same(bptree_session *bps, bptree_node *x, int i, void *k)
{
	if (i < 0)
		return 0;
	int rv = bptree_compar(bps, k, x->keys[i], &i,&i,sizeof(int),sizeof(int));
	if (rv == 0) return 1;
	return 0;
}
*/

// Experimental
// Independent of key or key-value; check in bps
int find_position_in_node(bptree_session *bps, bptree_node *x,
		bptree_key_val *kv, int *rv)
{
	int i;
	int cmp, prev;
	i = x->key_count-1;
	if(i < 0) {
		*rv = BPTREE_OP_KEY_NOT_FOUND;
		return 0;
	}
	cmp = prev = 1;

	while(i >= 0 && (cmp = bptree_compar_to_node(bps,x,kv,i)) >= 0)
	{
		i--;
		prev = cmp;
	}
	i++;
	*rv = (prev == 0 && x->active[i]) ? BPTREE_OP_KEY_FOUND : BPTREE_OP_KEY_NOT_FOUND;
	// FIXME Optmize this extra call to num_fields() out
	if(*rv == BPTREE_OP_KEY_FOUND && !x->leaf 
		&& bps->num_fields == num_fields_used(bps, kv))
	{
		i++;
	}
	return i;
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


/** Insert into a non-full bptree node
 * Returns:
 * BPTREE_OP_SUCCESS -- On success
 * BPTREE_ERR_DUPLICATE_KEY_INSERTED - if key existed (and we don't permit)
 * BPTREE_OP_FAIL - on invalid k/vsize
 * TODO Enumerate these!
 * errors from @read_node
 * errors from @write_node
 * BPTREE_OP_TAPIOCA_NOT_READY - if tapioca didn't grab the key yet
 * BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT - metanode didn't exist or corrupt
 */
static int bptree_insert_nonfull(bptree_session *bps,
		bptree_node* x, bptree_key_val *kv, int lvl)
{

	int pos, rv =BPTREE_OP_SUCCESS;

#ifdef DEFENSIVE_MODE
	assert(is_cell_ordered(bps,x));
	assert(are_key_and_value_sizes_valid(x));
#endif
	pos = find_position_in_node(bps, x, kv, &rv);
	if (pos < 0) pos = 0;
	assert(pos < BPTREE_NODE_SIZE );

#ifdef TRACE_MODE
	write_insert_to_trace(bps, x, kv, pos, lvl);
#endif

	if (x->leaf)
	{
		if (bps->insert_flags == BPTREE_INSERT_UNIQUE_KEY && 
			rv == BPTREE_OP_KEY_FOUND)
		{
			return BPTREE_ERR_DUPLICATE_KEY_INSERTED;
		}

		shift_bptree_node_elements_right(x,pos);
		copy_key_val_to_node(x, kv, pos);

		return write_node(bps, x);
	}
	else
	{
		bptree_node *n = NULL, *n2 = NULL;

		n = read_node(bps, x->children[pos], &rv);
		if (rv != BPTREE_OP_NODE_FOUND) return rv;
#ifdef TRACE_MODE
		if(!is_valid_traversal(bps, x,n,pos))
		{
			fflush(trace_fp);
			kill(getpid(),3);
			exit(1);
		}
#else
		assert(is_valid_traversal(bps, x,n,pos));
#endif

		if (node_is_full(n))
		{
			rv = bptree_split_child(bps, x, pos, n, lvl);
			if (rv != BPTREE_OP_SUCCESS) return rv;
			uuid_t new_node_id;
			uuid_copy(new_node_id, x->children[pos+1]);
			// FIXME Refactor out reference to bptree_compar
			if(bptree_compar_to_node(bps, x, kv, pos) < 0) pos++;

			n2 = read_node(bps, x->children[pos], &rv);
			if (rv != BPTREE_OP_NODE_FOUND) return rv;
			assert(!node_is_full(n2));
			// We should have got back either of the two split nodes
			assert(uuid_compare(x->children[pos], new_node_id) == 0 ||
					uuid_compare(x->children[pos], n->self_key) == 0);
			free_node(&n);
			n = n2;
		}

#ifdef DEFENSIVE_MODE
		assert(is_cell_ordered(bps,n));
#endif
		rv = bptree_insert_nonfull(bps, n, kv, lvl+1);
		free_node(&n);
		return rv;
	}
}

static int bptree_search_recursive(bptree_session *bps,
		bptree_node* x, bptree_key_val *kv)
{
	int i = 0, rv;
	bptree_node *n;
#ifdef DEFENSIVE_MODE
	assert(are_key_and_value_sizes_valid(x));
	assert(is_cell_ordered(bps,x));
#endif

	i = find_position_in_node(bps, x, kv, &rv);
	if (x->leaf)
	{
		if (bps->cursor_node != NULL) free_node(&(bps->cursor_node));
		bps->cursor_node = copy_node(x);
		bps->cursor_pos = i;
		bps->eof = 0;
		/*
		if (i >= x->key_count || i < 0)
		{
			kv->vsize = 0;
			return BPTREE_OP_KEY_NOT_FOUND;
		}
		*/
		if(rv == BPTREE_OP_KEY_FOUND && x->active[i])
		{
			memcpy(kv->v, x->values[i], x->value_sizes[i]);
			kv->vsize = x->value_sizes[i];
			//if (kv->ksize >= x->key_sizes[i]) bps->cursor_pos++;
		}
		else
		{
			kv->vsize = 0;
		}
		return rv;
	}
	else
	{
		n = read_node(bps, x->children[i], &rv);
		if (rv != BPTREE_OP_NODE_FOUND)
		{
			kv->vsize = 0;
			return rv;
		}
		assert(is_valid_traversal(bps, x, n, i));
		rv = bptree_search_recursive(bps, n, kv);
		free_node(&n);
		return rv;
	}
}
int bptree_index_next(bptree_session *bps, void *k,
		int32_t *ksize, void *v, int32_t *vsize)
{
	int rv ;
	if (bps->cursor_pos == -1) bps->cursor_pos++;
	if (bps->cursor_node == NULL)
	{
		bps->eof = 0;
		bps->cursor_pos = 0;
		*ksize = 0;
		*vsize = 0;
		//TODO This should probably be an error condition
		return BPTREE_OP_EOF;
	}

	rv = bptree_index_next_internal(bps, k, ksize,v, vsize);
	while (rv == BPTREE_OP_KEY_DELETED)
	{
		rv = bptree_index_next_internal(bps, k, ksize,v, vsize);
	}
	return rv;
}

int bptree_index_next_internal(bptree_session *bps, void *k,
		int32_t *ksize, void *v, int32_t *vsize)
{
	int rv;

	if(bps->eof) bps->eof = 0;

#ifdef DEFENSIVE_MODE
	assert(are_key_and_value_sizes_valid(bps->cursor_node));
	assert(bps->cursor_node->leaf == 1);
	assert(bps->cursor_pos >= 0 && bps->cursor_pos <= BPTREE_NODE_SIZE);
#endif

	while(!bps->cursor_node->active[bps->cursor_pos] &&
			bps->cursor_pos < bps->cursor_node->key_count -1) bps->cursor_pos++;

	if (bps->cursor_pos >= bps->cursor_node->key_count)
	{
		if(uuid_is_null(bps->cursor_node->next_node))
		{
			bps->eof = 1;
			*vsize = 0;
			*ksize = 0;
			bps->eof = 0;
			free_node(&(bps->cursor_node));
			return BPTREE_OP_EOF; // protect us from ourselves
		}
		else
		{
			// Read next cell
			bptree_node *next;
			next = read_node(bps, bps->cursor_node->next_node, &rv);
			if (rv != BPTREE_OP_NODE_FOUND) {
				*ksize = 0;
				*vsize = 0;
				return rv;
			}
			bps->cursor_pos = 0;
			free_node(&(bps->cursor_node));
			bps->cursor_node = next;
		}

	}

	if(!bps->cursor_node->active[bps->cursor_pos])
	{
		bps->cursor_pos++;
		return BPTREE_OP_KEY_DELETED;
	}

	memcpy(k, bps->cursor_node->keys[bps->cursor_pos],
			bps->cursor_node->key_sizes[bps->cursor_pos]);
	*ksize = bps->cursor_node->key_sizes[bps->cursor_pos];
	memcpy(v, bps->cursor_node->values[bps->cursor_pos],
			bps->cursor_node->value_sizes[bps->cursor_pos]);
	*vsize = bps->cursor_node->value_sizes[bps->cursor_pos];

	bps->cursor_pos++;

	return BPTREE_OP_KEY_FOUND;
}

/*@
 * An mget-equivalent for next(); attempt to read ahead up to number of rows
 * Replace *rows with the actual number of rows that were returned
 * **bmres points to the head of a linked list of results that were retrieved
 */
int bptree_index_next_mget(bptree_session *bps,
		bptree_mget_result **bmres, int16_t *rows, int *buf_sz)
{
	int rv, fetched;
	int32_t ksize, vsize;
	unsigned char k[MAX_TRANSACTION_SIZE/2];
	unsigned char v[MAX_TRANSACTION_SIZE/2];
	bptree_mget_result *cur, *next, *prev;
	cur = malloc(sizeof(bptree_mget_result));
	prev = *bmres = cur;
	rv = *buf_sz = 0;
	fetched = 1;
	cur->next = NULL;

	while (true)
	{
		rv = bptree_index_next(bps, k, &ksize, v, &vsize);
		if (rv == BPTREE_OP_EOF)
		{
			fetched--;
			if(cur->next != NULL) free(cur->next);
			if(prev->next != NULL) free(prev->next);
			cur->next = NULL;
			prev->next = NULL;
			break;
		}
		if (rv != BPTREE_OP_KEY_FOUND) {
			// error condition; free memory
			return rv;
		}

		*buf_sz += ksize + vsize;
		if (*buf_sz > MAX_TRANSACTION_SIZE) break;
		cur->k = malloc(ksize);
		cur->v = malloc(vsize);
		memcpy(cur->k, k, ksize);
		memcpy(cur->v, v, vsize);
		cur->ksize = ksize;
		cur->vsize = vsize;

		if (fetched >= *rows) break;

		next = malloc(sizeof(bptree_mget_result));
		cur->next = next;
		next->next = NULL;
		prev = cur;
		cur = next;
		fetched++;
	}
	*rows = fetched;
	return BPTREE_OP_KEY_FOUND;
}


/*@
 * Start at the front of the index but set the cursor so that the following
 * call to next() will start on the first key not the second.
 * For the sake of simplicity we'll just reuse the original index_first call
 * and move the pointer back one although this is a bit wasteful
 */
int bptree_index_first_no_key(bptree_session *bps)
{
	unsigned char k[MAX_TRANSACTION_SIZE], v[MAX_TRANSACTION_SIZE];
	int32_t ksize, vsize;
	int rv;
	rv = bptree_index_first(bps, k, &ksize, v, &vsize);
	if (rv != BPTREE_OP_KEY_FOUND) return rv;
	bps->cursor_pos--;
	// If our table has one row, EOF will be set, so, disable this
	bps->eof = 0;
	return rv;

}
int bptree_index_first(bptree_session *bps, void *k,
		int32_t *ksize, void *v, int32_t *vsize)
{
	int rv;
	bptree_node *root;
	bptree_meta_node *bpm = NULL;

	rv = bptree_read_root(bps, &bpm, &root);
	if (rv != BPTREE_OP_NODE_FOUND)
	{
		*ksize = 0;
		*vsize = 0;
		return rv;
	}

	rv = bptree_index_first_recursive(bps, k, ksize, v, vsize, root);
	if (rv == BPTREE_OP_KEY_DELETED)
	{
		rv = bptree_index_next(bps,k,ksize,v,vsize);
	}
	if(rv != BPTREE_OP_KEY_FOUND)
	{
		*ksize = 0;
		*vsize = 0;
	}
	free_node(&root);
	free_meta_node(&bpm);
	return rv;
}
static int bptree_index_first_recursive(bptree_session *bps, void *k,
		int32_t *ksize, void *v, int32_t *vsize, bptree_node *n)
{
	bptree_node *next;
	int rv;
#ifdef DEFENSIVE_MODE
	assert(are_key_and_value_sizes_valid(n));
#endif
	if (n->leaf)
	{
		if (n->key_count == 0)
		{
			bps->eof = 1;
			assert(bps->cursor_node == NULL); // nothing should have set
			return BPTREE_OP_EOF; // tree is empty
		}
		// In case our tree has only one element
		if (n->key_count == 1) bps->eof = 1;
		*ksize = n->key_sizes[0];
		*vsize = n->value_sizes[0];
		memcpy(k, n->keys[0], n->key_sizes[0]);
		memcpy(v, n->values[0], n->value_sizes[0]);
		bps->cursor_pos = 1;
		bps->cursor_node = copy_node(n);
		if(!n->active[0]) return BPTREE_OP_KEY_DELETED;
		return BPTREE_OP_KEY_FOUND;
	}
	else
	{
		next = read_node(bps, n->children[0], &rv);
		if (rv != BPTREE_OP_NODE_FOUND) return rv;
		rv = bptree_index_first_recursive(bps, k, ksize, v, vsize,
				next);
		free_node(&next);
		return rv;
	}
}

static int node_is_full(bptree_node* n)
{
	return (n->key_count == (BPTREE_NODE_SIZE));
}

/***
 * Methods for (un)marshalling b+tree cell and header data to byte buffers */
void * marshall_bptree_meta_node(bptree_meta_node *bpm, size_t *bsize)
{
	int i;
    msgpack_sbuffer *buffer = msgpack_sbuffer_new();
    msgpack_packer *pck = msgpack_packer_new(buffer, msgpack_sbuffer_write);
    msgpack_sbuffer_clear(buffer);

    msgpack_pack_int32(pck, bpm->execution_id);
    msgpack_pack_raw(pck, sizeof(uuid_t));
    msgpack_pack_raw_body(pck, bpm->root_key, sizeof(uuid_t));
    msgpack_pack_int16(pck, bpm->bpt_id);
	
	msgpack_packer_free(pck);
	*bsize = buffer->size;
	void *b = buffer->data;
	free(buffer);
	return b;
}

bptree_meta_node * unmarshall_bptree_meta_node(const void *buf, size_t sz)
{
    msgpack_zone z;
    msgpack_object obj;
    msgpack_unpack_return ret;
	int rv;
	size_t offset = 0;
	bptree_meta_node *bpm;
	
    msgpack_zone_init(&z, 4096);
	if (buf == NULL) return NULL;
	bpm = malloc(sizeof(bptree_meta_node));
	
    ret = msgpack_unpack(buf, sz, &offset,&z, &obj);
    bpm->execution_id =  (int32_t) obj.via.i64;
    ret = msgpack_unpack(buf, sz, &offset,&z, &obj);
    memcpy(bpm->root_key, obj.via.raw.ptr, sizeof(uuid_t));
    ret = msgpack_unpack(buf, sz, &offset,&z, &obj);
    bpm->bpt_id = (int16_t) obj.via.i64;
	
	msgpack_zone_destroy(&z);
	
	if (ret != MSGPACK_UNPACK_SUCCESS) return NULL;
	return bpm;

}

// Pack all non-dynamic array stuff first, in the order of the struct def
void * marshall_bptree_node(bptree_node *n, size_t *bsize)
{
	assert(is_node_sane(n));

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

//@ Wrapper for the various implementations of serialization we may have */
/* inline void * marshall_bptree_node(bptree_node *n, size_t *bsize)
{
#ifdef BPTREE_MARSHALLING_MSGPACK
	return marshall_bptree_node_msgpack(n,bsize);
#else
	return marshall_bptree_node_tpl(n,bsize);
#endif

}
*/


bptree_node * unmarshall_bptree_node(const void *buf,
		size_t sz, size_t *nsize)
{
    msgpack_zone z;
    msgpack_zone_init(&z, 4096);
    msgpack_object obj;
    msgpack_unpack_return ret;

	int i;
	size_t offset = 0;
	bptree_node *n = create_new_empty_bptree_node();
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


// I have no idea why I wrote this second method. 
bptree_node * unmarshall_bptree_node_msgpack2(const void *buf,
		size_t sz, size_t *nsize)
{
    msgpack_unpacked msg;
    msgpack_unpacked_init(&msg);
    msgpack_zone z;
    msgpack_zone_init(&z, 4096);

	int i, rv;
	size_t offset = 0;
	bptree_node *n = create_new_empty_bptree_node();
	*nsize = sizeof(bptree_node);

    rv = msgpack_unpack_next(&msg, buf, sz, &offset);
    memcpy(n->self_key, msg.data.via.raw.ptr, sizeof(uuid_t));
    rv = msgpack_unpack_next(&msg, buf, sz, &offset);
    n->key_count =  (int16_t) msg.data.via.i64;
    rv = msgpack_unpack_next(&msg, buf, sz, &offset);
    n->leaf =  (int16_t) msg.data.via.i64;
    rv = msgpack_unpack_next(&msg, buf, sz, &offset);
    memcpy(n->parent, msg.data.via.raw.ptr, sizeof(uuid_t));
    rv = msgpack_unpack_next(&msg, buf, sz, &offset);
    memcpy(n->prev_node, msg.data.via.raw.ptr, sizeof(uuid_t));
    rv = msgpack_unpack_next(&msg, buf, sz, &offset);
    memcpy(n->next_node, msg.data.via.raw.ptr, sizeof(uuid_t));
    rv = msgpack_unpack_next(&msg, buf, sz, &offset);
    memcpy(n->active, msg.data.via.raw.ptr, BPTREE_NODE_SIZE);

    // Only unpack data that is relevant based on the # of keys in this node

	for (i = 0; i < n->key_count; i++)
	{
		rv = msgpack_unpack_next(&msg, buf, sz, &offset);
		n->key_sizes[i] = (int32_t) msg.data.via.i64;
	}
	for (i = 0; i < n->key_count; i++)
	{
		rv = msgpack_unpack_next(&msg, buf, sz, &offset);
		n->value_sizes[i] = (int32_t) msg.data.via.i64;
	}
	for (i = 0; i < n->key_count; i++)
	{
		rv = msgpack_unpack_next(&msg, buf, sz, &offset);
		*nsize+= msg.data.via.raw.size;
		n->keys[i] = malloc(msg.data.via.raw.size);
		memcpy(n->keys[i], msg.data.via.raw.ptr, msg.data.via.raw.size);
	}
	for (i = 0; i < n->key_count; i++)
	{
		rv = msgpack_unpack_next(&msg, buf, sz, &offset);
		*nsize+= msg.data.via.raw.size;
		n->values[i] = malloc(msg.data.via.raw.size);
		memcpy(n->values[i], msg.data.via.raw.ptr, msg.data.via.raw.size);
	}
	if (!n->leaf)
		for (i = 0; i <= n->key_count; i++)
		{
			rv = msgpack_unpack_next(&msg, buf, sz, &offset);
			memcpy(n->children[i], msg.data.via.raw.ptr, sizeof(uuid_t));
		}

	msgpack_unpacked_destroy(&msg);
	return n;
}


int bptree_read_root(bptree_session *bps, bptree_meta_node **bpm,
		bptree_node **root)
{
	int rv;

	*bpm = read_meta_node(bps,&rv);
	if (rv != BPTREE_OP_NODE_FOUND) return rv;

	*root = read_node(bps, (*bpm)->root_key, &rv);
	if (rv != BPTREE_OP_NODE_FOUND) return rv;

	assert(uuid_is_null((*root)->parent));
	assert(uuid_compare((*bpm)->root_key, (*root)->self_key) == 0);

	return rv;
}

//inline
int is_node_sane(bptree_node *n)
{
	int i =0;
	// TODO Embed all the asserts inside this function
	// Do some sanity checking of the node; these two checks should
	// probably be enough to catch most cases of corrupt or uninitialized nodes
	if (!n->node_active) return 1; // Skip these checks if node is deleted
	if (n == NULL) return 0;
	if (n->key_count < 0 || n->key_count > BPTREE_NODE_SIZE) return 0;
	// Ensure we don't have any weird corner cases with the children of node
	if (!n->leaf && n->key_count > 0)
		if (uuid_is_null(n->children[n->key_count]) ||
				uuid_is_null(n->children[0])) return 0;

	for (i = 0; i < n->key_count; i++)
	{
		if(n->keys[i] == NULL) return 0;
		if(n->values[i] == NULL) return 0;
		if(n->key_sizes[i] < 0) return 0;
		if(n->value_sizes[i] < 0) return 0;
	}

	if(!n->leaf) {
		for (i = 0; i <= n->key_count; i++) {
			if(uuid_is_null(n->children[i])) return 0;
		}
	}

	// Make sure anything outside boundaries is not filled with anything
	for (i = n->key_count; i < BPTREE_NODE_SIZE; i++)
	{
		if(n->keys[i] != NULL || n->values[i] != NULL) return 0;
		if(n->key_sizes[i] != -1 || n->value_sizes[i] != -1) return 0;
		if(!uuid_is_null(n->children[i+1])) return 0;
	}
	if(n->key_count < BPTREE_NODE_SIZE &&
			!uuid_is_null(n->children[BPTREE_NODE_SIZE]) ) return 0;
	return 1;
}

int is_correct_node(bptree_node *n, uuid_t node_key)
{
	// Did we actually get the right node back?
	if (n == NULL || node_key == NULL) return 0;
	return uuid_compare(n->self_key, node_key) == 0;
}

/**
 * Returns:
 * BPTREE_OP_TAPIOCA_NOT_READY - if tapioca key not ready yet
 * BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT - on other err
 * BPTREE_OP_NODE_FOUND - on success
 */
bptree_node * read_node(bptree_session *bps,
		uuid_t node_key, int *rv)
{
	int rv2;
	size_t ksize, nsize, vsize;
	bptree_node *n = NULL;
	key _k;
	val *_v;
	void *k, *v;
	unsigned char v_comp[BPTREE_MAX_VALUE_SIZE];

	k = create_cell_key(bps, node_key, &ksize);
	_k.data = k;
	_k.size = ksize;
	_v = transaction_get(bps->t, &_k);
	free(k);
	if (_v == NULL)
	{
		*rv = BPTREE_OP_TAPIOCA_NOT_READY;
	} else if (_v->size == 0)
	{
		uuid_unparse(node_key, uudbg);
		printf("Error %d attempting to read bpt %d, size %d key %s\n",
				BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT, bps->bpt_id,
				_v->size, uudbg);
		fflush(stdout);
		*rv = BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT;
	} else
	{
		assert(_v->version <= bps->t->st);
		v = _v->data;
		vsize = _v->size;

		if (BPTREE_NODE_COMPRESSION)
		{
		    z_stream strm;
		    strm.zalloc = Z_NULL;
		    strm.zfree = Z_NULL;
		    strm.opaque = Z_NULL;
		    rv2 = inflateInit(&strm);
		    strm.avail_in = _v->size;
		    strm.next_in = _v->data;
		    strm.next_out = v_comp;
		    strm.avail_out = BPTREE_MAX_VALUE_SIZE;
		    rv2 = inflate(&strm, Z_FINISH);
		    assert(rv2 != Z_STREAM_ERROR);  /* state not clobbered */
		    assert(strm.avail_in == 0); // all input was used
		    vsize = BPTREE_MAX_VALUE_SIZE - strm.avail_out;
		    v = v_comp;
		    inflateEnd(&strm);
		}
		n = unmarshall_bptree_node(v, vsize, &nsize);
		assert(nsize >= sizeof(bptree_node));
		val_free(_v);
		if(n == NULL)
		{
			*rv = BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT;
			return NULL;
		}
		*rv = BPTREE_OP_NODE_FOUND;
	}
	return n;

#ifdef DEFENSIVE_MODE
	assert(is_correct_node(n, node_key));
	assert(is_node_sane(n));
	assert(is_cell_ordered(bps,n));
	assert(are_key_and_value_sizes_valid(n));
#endif
	return n;
}

/**
 * Write a bptree node
 * Returns:
 * BPTREE_OP_FAIL - on failure to write (probably impossible inside tapioca..)
 * BPTREE_OP_SUCCESS - on success
 */
int write_node(bptree_session *bps, bptree_node* n)
{
	size_t ksize, vsize;
	int rv;
	void *k;
	void *v;
	key _k;
	val _v;
	unsigned char v_comp[BPTREE_MAX_VALUE_SIZE];

#ifdef DEFENSIVE_MODE
	assert(is_node_sane(n));
	assert(is_cell_ordered(bps,n));
#endif

	k = create_cell_key(bps, n->self_key, &ksize);
	v = marshall_bptree_node(n, &vsize);
	if (k == NULL || v == NULL) return -1;
	if (BPTREE_NODE_COMPRESSION)
	{
		z_stream strm;
	    strm.zalloc = Z_NULL;
	    strm.zfree = Z_NULL;
	    strm.opaque = Z_NULL;
	    rv = deflateInit(&strm, COMPRESSION_LEVEL);
	    strm.avail_in = vsize;
	    strm.next_in = v;
	    strm.next_out = v_comp;
	    strm.avail_out = BPTREE_MAX_VALUE_SIZE;
	    rv = deflate(&strm, Z_FINISH);
	    assert(rv != Z_STREAM_ERROR);
	    assert(strm.avail_in == 0); // all input was used
	    // Now what is the size of our stream?

	    vsize = BPTREE_MAX_VALUE_SIZE - strm.avail_out;
	    v = realloc(v, vsize);
	    memcpy(v, v_comp, vsize);
	    deflateEnd(&strm);

	}
	_k.data = k;
	_k.size = ksize;
	_v.data = v;
	_v.size = vsize;

	rv = transaction_put(bps->t, &_k,&_v);

#ifdef TRACE_MODE
	//write_to_trace_file(0,&(bps->t->id), &_k, &_v, prev_client_id);
#endif
	free(k);
	free(v);
	if (rv == 0) return BPTREE_OP_SUCCESS;
	return BPTREE_OP_FAIL;
}

/** Copy all memory associated with node and return a new pointer */
// TODO The probability that i implemented this correctly on teh first try
// are not good. Double check.
bptree_node *copy_node(bptree_node *n)
{
	assert(is_node_sane(n));
	bptree_node *x;
	//x = malloc(sizeof(bptree_node));
	// Ensure we use the standard way of creating a new , uninitialized node
	x = create_new_empty_bptree_node();
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

void free_meta_node(bptree_meta_node **m)
{
	free(*m);
	*m = NULL;
}

/** Reset/create contents of bptree handle; will need to be commited after
 * Returns:
 * BPTREE_OP_NODE_FOUND -- On success
 * BPTREE_OP_TAPIOCA_NOT_READY - if tapioca didn't grab the key yet
 * BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT - metanode didn't exist or corrupt
 */
bptree_meta_node * read_meta_node(bptree_session *bps, int *rv)
{
	size_t ksize;
	bptree_meta_node *m = NULL;
	void *k;
	key _k;
	val *_v;

	k = create_meta_key(bps, &ksize);
	_k.data = k;
	_k.size = ksize;
	_v = transaction_get(bps->t, &_k);
	if(_v == NULL)
	{
		*rv = BPTREE_OP_TAPIOCA_NOT_READY;
	}
	//else if ( _v->size <= sizeof(bptree_meta_node))
	else if ( _v->size <= sizeof(uuid_t))
	{
		*rv = BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT;
	}
	else
	{
		assert(_v->version <= bps->t->st);
		m = unmarshall_bptree_meta_node(_v->data, _v->size);
		m->last_version = _v->version;
		val_free(_v);
		if(m == NULL) *rv = BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT;
		*rv = BPTREE_OP_NODE_FOUND;
	}
	free(k);
	return m;
}

/*@ To be extra paranoid, re-write the new root node when we write the meta node
 * In any case where we have to write this meta node the new root will be part
 * of the writeset so this should not be so much more expensive
 */
int write_meta_node(bptree_session *bps, bptree_meta_node* bpm, bptree_node *root)
{
	size_t ksize, vsize;
	void *k;
	void *v;
	key _k;
	val _v;

	k = create_meta_key(bps, &ksize);
	v = marshall_bptree_meta_node(bpm, &vsize);
	_k.data = k;
	_k.size = ksize;
	_v.data = v;
	_v.size = vsize;

	transaction_put(bps->t, &_k, &_v);

	if (root != NULL)
	{
		assert(uuid_compare(bpm->root_key, root->self_key) == 0);
		assert(uuid_is_null(root->parent));
		if (write_node(bps, root) != BPTREE_OP_SUCCESS) return BPTREE_OP_FAIL;
	}
#ifdef TRACE_MODE
	write_to_trace_file(0,&(bps->t->id), &_k, &_v, 0);
#endif
	free(k);
	free(v);
	return BPTREE_OP_SUCCESS;
}

// In searches, assert that the tree obeys the properties we expect
void assert_valid_parent_child_traversal(bptree_session *bps,
		bptree_node *p, bptree_node *c, int i)
{
	// If c is a leaf, then the first key of c should match p[i]
	bptree_key_val *kv_p, *kv_c;
	get_key_val_from_node(p, i, kv_p);
	get_key_val_from_node(c, i, kv_c);
	if (c->leaf) {
		if(bptree_compar_keys(bps, kv_p, kv_c) != 0) assert(0);
	}
	// if c is not a leaf, then c[0] should be strictly greater than p[i]
	else
	{
		if(bptree_compar_keys(bps, kv_p, kv_c) <= 0) assert(0);
	}
}

#ifdef TRACE_MODE

void get_trace_file(void)
{
	if (!file_opened)
	{
		char fl[128];
		sprintf(fl, "/tmp/tapioca-%d.trc", getpid());
		trace_fp = fopen((const char *)fl, "w");
		file_opened = 1;
	}
}

void prepend_client_and_whitespace(bptree_session *bps, int lvl)
{
	int i;
	fprintf(trace_fp, "%d |", bps->tapioca_client_id);
	for (i=0; i<lvl; i++) fprintf(trace_fp, "  ");
}

/*@ Write trace-appropriate data into s for the given node */
void write_node_info(bptree_session *bps, char *s, bptree_node *n)
{
	int i;
	char *sptr = s;
	char uu[40];
	char c[512];
	bzero(c,512);
	uuid_unparse(n->self_key, uu);
	uu[8] = '\0'; // print first 8 bytes...
	for (i=0; i<n->key_count; i++)
	{
		bptree_key_value_to_string(bps, n->keys[i], n->values[i],
				n->key_sizes[i], n->value_sizes[i], c);
		// Unsafe. Oh well
		strcpy(sptr, c);
		sptr += strlen(c);
		sprintf(sptr, ", ");
		sptr += 2;
	}
	if (n->key_count > 0)
	{
		sptr -= 2;
		bzero(sptr, 2);
	}
	sprintf(sptr, " Cnt: %d L: %d ID: %s WC: %d Ver: %d ST: %d ",
			n->key_count, n->leaf, uu, n->write_count, n->last_version,
			bps->t->st);
}

int write_insert_to_trace(bptree_session *bps, bptree_node *x,
		bptree_key_val *kv, int pos, int lvl)
{
	int i;
	unsigned char *kk, *vv;
	int16_t bpt_id;
	size_t nsize;
	char uu[40],res[20];
	char nodestr[4096];
	get_trace_file();

	bptree_key_value_to_string_kv(bps, kv, nodestr);
	prepend_client_and_whitespace(bps, lvl);
	fprintf(trace_fp, "Insert key: %s pos %d lvl %d\n", nodestr, pos, lvl);
	bzero(nodestr, 4096);

	// X
	prepend_client_and_whitespace(bps, lvl);
	write_node_info(bps, nodestr, x);
	fprintf(trace_fp, "X  : %s\n", nodestr);

	bzero(nodestr, 4096);

	return 0;
}

int write_split_to_trace(bptree_session *bps, bptree_node *x, bptree_node *y,
		bptree_node *new, int pos, int lvl)
{
	int i;
	unsigned char *kk, *vv;
	int16_t bpt_id;
	size_t nsize;
	char uu[40],res[20];
	char nodestr[4096];
	get_trace_file();

	prepend_client_and_whitespace(bps, lvl);
	if (new == NULL) fprintf(trace_fp, "Pre-Split pos %d lvl %d\n", pos, lvl);
	if (new != NULL) fprintf(trace_fp, "Post-Split pos %d lvl %d\n", pos, lvl);

	// X
	prepend_client_and_whitespace(bps, lvl);
	write_node_info(bps, nodestr, x);
	fprintf(trace_fp, "X  : %s\n", nodestr);
	bzero(nodestr, 4096);

	// Y
	prepend_client_and_whitespace(bps, lvl);
	write_node_info(bps, nodestr, y);
	fprintf(trace_fp, "Y  : %s\n", nodestr);
	bzero(nodestr, 4096);

	// New
	if(new != NULL)
	{
		prepend_client_and_whitespace(bps, lvl);
		write_node_info(bps, nodestr, new);
		fprintf(trace_fp, "New: %s\n", nodestr);
	}

	return 0;
}

int write_to_trace_file(int type,  tr_id *t, key* k, val* v, int prev_client)
{
	bptree_node *n;
	bptree_meta_node *m;
	unsigned char *kk, *vv;
	int16_t bpt_id;
	size_t nsize;
	char uu[40],res[20];
	get_trace_file();
	switch (type) {
		// Three different commit-types (1,-1,-2)
		case T_COMMITTED:
			fprintf(trace_fp, "%d | COMMIT\n", t->client_id);
			break;
		case T_ABORTED:
			fprintf(trace_fp, "%d | ABORT\n", t->client_id);
			break;
		case T_ERROR:
			fprintf(trace_fp, "%d | ERROR\n", t->client_id);
			break;
		case 0: // o/w this was a put
			kk = k->data;
			vv = v->data;
			if(*kk == BPTREE_NODE_PACKET_HEADER)
			{
				memcpy(&bpt_id, kk+1, sizeof(int16_t));
				n = unmarshall_bptree_node(vv, v->size, &nsize);
				uuid_unparse(n->self_key, uu);
				fprintf(trace_fp, "%d |\tBPT:%d\t%s\tCnt:%d\tLeaf:%d"
									"\tWriteCnt %d P/C Cl_id %d,%d\n",
						t->client_id, bpt_id, uu, n->key_count, n->leaf,
						n->write_count, prev_client, n->tapioca_client_id );
			} else if(*kk == BPTREE_META_NODE_PACKET_HEADER)
			{
				memcpy(&bpt_id, kk+1, sizeof(int16_t));
				m = unmarshall_bptree_meta_node(vv, v->size);
				uuid_unparse(m->root_key, uu);
				fprintf(trace_fp, "%d |\tBPT:%d\tMeta:%s\n",
						t->client_id, bpt_id, uu);
			}

			break;
		default:
			break;
	}
	return 0;
}
#endif


void dump_node_info(bptree_session *bps, bptree_node *n)
{
	int i;
	char s1[512];
	char uuid_out[40];
	bptree_key_val kv;
	if (n == NULL) {
		printf ("\tB+tree node is null!\n");
		return;
	}
	if (!is_bptree_node_sane(n))
		printf("\tB+tree node failed sanity check!!!");

	printf("\tLeaf: %d\n", n->leaf);

	uuid_unparse(n->self_key, uuid_out);
	printf("\tSelf key: %s\n", uuid_out);
	uuid_unparse(n->parent, uuid_out);
	printf("\tParent key: %s\n", uuid_out);
	uuid_unparse(n->next_node, uuid_out);
	printf("\tNext key: %s\n", uuid_out);
	uuid_unparse(n->prev_node, uuid_out);
	printf("\tPrev key: %s\n", uuid_out);

	if (!are_key_and_value_sizes_valid(n))
		printf("\tB+tree node k/v sizes are invalid!!!");

	printf("\tKey count: %d\n", n->key_count);
	printf("\tKey/value sizes: ");
	for (i =0; i < n->key_count; n++)
		printf ("(%d, %d), ", n->key_sizes[i], n->value_sizes[i]);

	printf("\n\tKeys:\n");
	for (i =0; i < n->key_count; n++)
	{
		get_key_val_from_node(n, i, &kv);
		bptree_key_value_to_string_kv(bps, &kv, s1);
		printf(" %s ", s1);
	}

	if(!n->leaf)
	{
		printf("\n\tChild nodes:\n");
		for (i =0; i <= n->key_count; n++)
		{
			uuid_unparse(n->children[i], uuid_out);
			printf("\t\t%d: %s \n", i, uuid_out);
		}
	}

	printf("\n");

	fflush(stdout);
}

/** See get_unique_key() for details on how the cell_id is generated;
 *  To persist to tapioca we simply prepend the header and bpt_id to create an
 *  key. Responsibility of caller to free memory in k once done!
 */
void * create_cell_key(bptree_session *bps, uuid_t cell_id, size_t *ksize)
{
	void *k;
	k = malloc(sizeof(uuid_t) + sizeof(int16_t) + sizeof(unsigned char));
	unsigned char *kptr = k;
	*ksize = sizeof(uuid_t) + sizeof(int16_t) + sizeof(unsigned char);
	const unsigned char hdr = BPTREE_NODE_PACKET_HEADER;
	memcpy(kptr, &hdr, sizeof(unsigned char));
	kptr += sizeof(unsigned char);
	memcpy(kptr, &bps->bpt_id, sizeof(uint16_t));
	kptr += sizeof(uint16_t);
	memcpy(kptr, cell_id, sizeof(uuid_t));
	return k;
}

// Meta key is just the header + the bpt_id; should be enough to distinguish
void * create_meta_key(bptree_session *bps, size_t *ksize)
{
	void *k;
	k = malloc(sizeof(int16_t) + sizeof(unsigned char));
	unsigned char *kptr = k;
	*ksize = sizeof(int16_t) + sizeof(unsigned char);
	const unsigned char hdr = BPTREE_META_NODE_PACKET_HEADER;
	memcpy(kptr, &hdr, sizeof(unsigned char));
	kptr += sizeof(unsigned char);
	memcpy(kptr, &bps->bpt_id, sizeof(uint16_t));
	return k;
}

int is_valid_traversal(bptree_session *bps, bptree_node *x, bptree_node *n,int i)
{
	int rv, c;
/*	 Similar to our split cells method;
 *  If we are traversing to a leaf node, the traversal key should be the same
 *   as the first key in the child, otherwise, it should be strictly greater
 *    Two other cases:
 *    i = 0 or i = x->key_count are boundary cases
	 */
#ifndef DEFENSIVE_MODE
	return 1;
#endif

	if(x->key_count <= 0) return 1;

	if (i == 0)
	{
		// If this is a left hand traversal, the max value of n should be
		// strictly less than the first key of x
		c = n->key_count -1;
		rv = bptree_compar(bps, x->keys[0], n->keys[c],
				x->values[0], n->values[c],
				x->value_sizes[0], n->value_sizes[c], bps->num_fields);
		assert (rv > 0);
		//if (rv <= 0) return 0;
		return 1;
	}
	else
	{
//		if(i == x->key_count) c--;
		c = i-1;
		rv = bptree_compar(bps, x->keys[c], n->keys[0],
				x->values[c], n->values[0],
				x->value_sizes[c], n->value_sizes[0], bps->num_fields);
	}
	if (n->leaf)
	{
		assert(rv == 0);
		if (rv != 0) return 0;
	}
	else
	{
		assert(rv < 0);
		if (rv >= 0) return 0;
	}

	return 1;
}

int are_split_cells_valid(bptree_session *bps, bptree_node* x, int i,
		bptree_node *y, bptree_node *n)
{
	// Same var names as split;
	// x should be parent of y and new (y: left, new : right)
	int rv;
//	size_t sz;
	/*
	     	   N   W
	 N  P  Q  R  S  T  U  V  Z
	 should become, in the case where y is a leaf:

	        N     S     W
	           |     |
	   N  P  Q  R      S  T  U  V  Z

	   and if y is not a leaf

	        N     S     W
	           |     |
	   N  P  Q  R      T  U  V  Z

	 */

	if (y->key_count > 0) {
		// In rare cases, y could be empty
		rv = bptree_compar(bps, y->keys[y->key_count - 1], x->keys[i],
				y->values[y->key_count - 1], x->values[i],
				y->value_sizes[y->key_count - 1], x->value_sizes[i], 
				bps->num_fields);
		//if (rv >= 0) return 0; // i.e. R strictly less than S
		assert(rv < 0);
	}

	rv = bptree_compar(bps, x->keys[i], n->keys[0],
			x->values[i], n->values[0],
			x->value_sizes[i], n->value_sizes[0], 
			bps->num_fields);

	// Rewrite these checks as asserts
	//if (y->leaf && rv != 0) return 0; // S should be in root and new[0]
	//if (!y->leaf && rv >= 0) return 0; // i.e. T strictly less than S
	assert (y->leaf ? rv == 0 : rv < 0); // S should be in root and new[0]
//	assert (!y->leaf && rv < 0); // i.e. T strictly less than S

//	if (y->key_count != BPTREE_MIN_DEGREE-1 ||
	//		n->key_count != BPTREE_MIN_DEGREE -1 + n->leaf) return 0;
	assert(y->key_count == BPTREE_MIN_DEGREE-1 );
	assert(n->key_count == BPTREE_MIN_DEGREE -1 + n->leaf);
	return 1;
}
//inline
int is_cell_ordered(bptree_session *bps, bptree_node* y)
{
	int i, rv;
//	size_t sz;
	void *a, *b, *v1, *v2;
	for (i = 0; i < y->key_count - 1; i++)
	{
		a = y->keys[i];
		b = y->keys[i + 1];
		v1 = y->values[i];
		v2 = y->values[i + 1];
		rv = bptree_compar(bps, a, b, v1, v2,
				y->value_sizes[i], y->value_sizes[i+1], bps->num_fields);
		//if (rv >= 0) return 0; // if two kev/values are the same we have a prob!
		assert(rv < 0); // if two kev/values are the same we have a prob!
	}

	return 1;
}

int is_bptree_node_sane(bptree_node* n)
{
	// For now just make sure that we have a valid child for a non leaf
	if (!n->leaf && uuid_is_null(n->children[n->key_count])) return 0;
	return 1;

}

int are_key_and_value_sizes_valid(bptree_node* n)
{
	int i;
	for (i = 0; i < n->key_count; i++)
	{
		if (n->key_sizes[i] > BPTREE_MAX_VALUE_SIZE
			|| n->value_sizes[i]> BPTREE_MAX_VALUE_SIZE
			|| n->key_sizes[i]< -1
			|| n->value_sizes[i] < -1)
			return 0;
	}

	return 1;
}

void print_trace (void)
{
  void *array[10];
  size_t size;
  char **strings;
  size_t i;

  size = backtrace (array, 10);
  strings = backtrace_symbols (array, size);

  printf ("Obtained %zd stack frames.\n", size);

  for (i = 0; i < size; i++)
     printf ("%s\n", strings[i]);

  free (strings);
  printf("Trying to write with backtrace_symbols_fd to stdout:\n");
  backtrace_symbols_fd(array, size, STDOUT_FILENO);
}

/*@ Wrapper for uuid_generate */
static void get_unique_key(bptree_session *bps, uuid_t uu)
{
	uuid_generate(uu);
}

//***** Comparison Functions */

// TODO This is quick and convenient now, but we need to find a better way to
// implement these various comparison functions
//inline

int int8cmp(const void *i1, const void *i2, size_t v_ignored)
{
	int8_t *a = (int8_t*) i1;
	int8_t *b = (int8_t*) i2;
	if (*a < *b) { return -1; }
	else if (*a > *b) { return 1; }
	else { return 0; }
}
inline int int16cmp(const void *i1, const void *i2, size_t v_ignored)
{
	int16_t *a = (int16_t*) i1;
	int16_t *b = (int16_t*) i2;
	if (*a < *b) { return -1; }
	else if (*a > *b) { return 1; }
	else { return 0; }
}

inline int int32cmp(const void *i1, const void *i2, size_t v_ignored)
{
	int32_t *a = (int32_t*) i1;
	int32_t *b = (int32_t*) i2;
	if (*a < *b) { return -1; }
	else if (*a > *b) { return 1; }
	else { return 0; }
}

inline int int64cmp(const void *i1, const void *i2, size_t v_ignored)
{
	int64_t *a = (int64_t*) i1;
	int64_t *b = (int64_t*) i2;
	if (*a < *b) { return -1; }
	else if (*a > *b) { return 1; }
	else { return 0; }
}

/* TODO Integrate this more sensibly with the collation stuff in mysql;
 * for now we just need a quick and easy way to hack off the first 1-2 bytes
 * that include the string length for <255 or <64k length strings*/
inline int strncmp_mysql(const void *i1, const void *i2, size_t sz)
{
	size_t len = sz - (sz > 255 ? 2 : 1);
	const char *a = (const char*) i1 + (sz > 255 ? 2 : 1);
	const char *b = (const char*) i2 + (sz > 255 ? 2 : 1);;
	return strncmp(a,b,len);
}

// VARCHARs in MySQL always use 2-byte lengths
int strncmp_mysql_var(const void *i1, const void *i2, size_t sz)
{
	size_t len = sz - 2;
	const char *a = (const char*) i1 + 2;
	const char *b = (const char*) i2 + 2;
	return strncmp(a,b,len);
}

int strncmp_wrap(const void *i1, const void *i2, size_t sz)
{
	return strncmp((char *)i1,(char *)i2,sz);
}
