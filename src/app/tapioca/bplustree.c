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

#include "bplustree.h"
#include "bptree_node.h"
#include "tcp.h"

int file_opened =0;
FILE *trace_fp;

static char uudbg[40];

int bptree_search_kv(bptree_session *bps, void *k,
		int32_t ksize, void *v, int32_t vsize);
static int bptree_search_recursive(bptree_session *bps,
		bptree_node* n, bptree_key_val *kv);
static int bptree_insert_nonfull(bptree_session *bps,
		bptree_node* x, bptree_key_val *kv, int lvl);
static int bptree_update_recursive(bptree_session *bps,
		bptree_node* x,bptree_key_val *kv);
static int bptree_index_first_recursive(bptree_session *bps,
					bptree_key_val *kv, bptree_node *n);
static int 
bptree_delete_recursive(bptree_session *bps, bptree_node* x, bptree_key_val *kv);
void * create_cell_key(bptree_session *bps, uuid_t cell_id, size_t *ksize);
void * create_meta_key(bptree_session *bps, size_t *ksize);
static void get_unique_key(bptree_session *bps, uuid_t uu);
bptree_meta_node * create_bptree_or_reset(bptree_session *bps, int *rv);
int bptree_index_next_internal(bptree_session *bps, bptree_key_val *kv);


inline int num_fields_used(bptree_session *bps, const bptree_key_val *kv) ;

void traversal_check(bptree_session *bps, bptree_node *x, bptree_node *n,int i);
void reposition_cursor_before_redistrib(bptree_session *bps, bptree_node *cl, 
					bptree_node *cr);

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

void clear_cursor(bptree_session *bps)
{
	if(bps->cursor_node != NULL)
	{
		free_node(&(bps->cursor_node));
	}
	bps->cursor_pos = 0;
	bps->eof = 0;
	
}

int bptree_set_active_bpt_id(bptree_session *bps, tapioca_bptree_id bpt_id)
{
	bps->bpt_id = bpt_id;
	bps->eof = 0;
	bps->cursor_pos = 0;
	bps->cursor_node = NULL;
	return BPTREE_OP_SUCCESS;
}

void bptree_free_session(bptree_session **bps)
{
	free((*bps)->bfield);
	transaction_destroy((*bps)->t);
	*bps = NULL;
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

	root = bpnode_new();

	bptree_meta_node bpm;
	bpm.bpt_id = bps->bpt_id;
	bpm.execution_id = 1;
	uuid_copy(bpm.root_key, bpnode_get_id(root));

	if (write_meta_node(bps, &bpm, root) < 0)
	{
		*rv = BPTREE_OP_METADATA_ERROR;
		return NULL;
	}
	free(root);
	return read_meta_node(bps, rv);

}

int bptree_search_kv(bptree_session *bps, void *k,
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

	if (ksize <= 0 || ksize >= 65000) return BPTREE_OP_INVALID_INPUT;
	
	rv = bptree_read_root(bps, &bpm, &root);
	if (rv != BPTREE_OP_NODE_FOUND) return rv;

	rv = bptree_search_recursive(bps, root, &kv);

	free_node(&root);
	free_meta_node(&bpm);
	return rv;
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
	kv.vsize = 0;

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
	
	if (bpnode_size(root) == 0 && !bpnode_is_leaf(root)) {
		// Set new root
		uuid_copy(bpm->root_key, bpnode_get_child_id(root, 0));
		bpnode_clear_parent(root);
		if (write_meta_node(bps, bpm, NULL) < 0) 
			return BPTREE_OP_METADATA_ERROR;
	}

	free_node(&root);
	free_meta_node(&bpm);
	return rv;
}

static int 
bptree_delete_recursive(bptree_session *bps, bptree_node* x, bptree_key_val *kv)
{
	int k_pos, c_pos,rv, rv_f;

#ifdef DEFENSIVE_MODE
	assert(is_node_ordered(bps,x) == 0);
#endif

	rv_f = find_position_in_node(bps, x, kv, &k_pos, &c_pos);
	
	if (bpnode_is_leaf(x))
	{
		if (rv_f == BPTREE_OP_KEY_FOUND)
		{
			/* Check if we need to invalidate the cached node 
			 * a cursor might be on */
			if(bpnode_is_same(x, bps->cursor_node)
				&& bps->cursor_pos >= k_pos)
			{
				bps->cursor_pos--;
				bps->cached_key_dirty = 1;
			}
			
			delete_key_from_node(x,k_pos);
			
			if (write_node(bps, x) != BPTREE_OP_SUCCESS) 
				return BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT;
		}
		return rv_f;
	}
	else
	{
		bptree_node *c;

		c = read_node(bps, bpnode_get_child_id(x, c_pos), &rv);
		if (rv != BPTREE_OP_NODE_FOUND) return rv;
		
		rv = bptree_delete_recursive(bps, c, kv);
		
		// Check if we have an underflow condition
		if(is_bptree_node_underflowed(c))
		{
			rebalance_nodes(bps, x, c, c_pos);
		}
		
		if (rv_f == BPTREE_OP_KEY_FOUND)
		{
			bpnode_set_inactive(x, k_pos);
			if (write_node(bps, x) != BPTREE_OP_SUCCESS)
				return BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT;
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
	if (i >= bpnode_size(p)) {
		i_adj = i-1;
	}
	cl = c;
	cr = adj = read_node(bps, bpnode_get_child_id(p, i_adj), &rv);
	if (rv != BPTREE_OP_NODE_FOUND) return rv;
	assert(!is_bptree_node_underflowed(adj));
	
	if (i_adj < i)
	{
		cl = adj;
		cr = c;
	}
	assert(bpnode_is_leaf(cl) == bpnode_is_leaf(cr));
	
	int sp_i = (i < i_adj) ? i : i_adj;
	if (bpnode_size(c) + bpnode_size(adj) < BPTREE_NODE_SIZE)
	{
		// Reposition cursor if we clear the node it was on
		if(bpnode_is_same(cr, bps->cursor_node)) 
		{
			cache_node_in_session(bps, cl);
			bps->cached_key_dirty = 1;
			bps->cursor_pos = bpnode_size(cl) + bps->cursor_pos; 
		}
		// Concatenate (i.e. remove one) nodes
		concatenate_nodes(p, cl, cr, sp_i);
		
	} 
	else 
	{
		reposition_cursor_before_redistrib(bps, cl, cr);
		// Redistribute keys among nodes
		redistribute_keys(p, cl, cr, sp_i);
	}
	
	// cl is about to become the new root, clear parent before writing back
	if (bpnode_size(p) == 0) {
		bpnode_clear_parent(cl);
	}
	
	rv = write_3_nodes(bps, p,cl,cr);
	free_node(&adj);
	
	return rv;
}

/* When we redistribute keys, we need to ensure that the cursor has been 
 * repositioned to avoid holes or duplicate reads to the client */
void reposition_cursor_before_redistrib(bptree_session *bps, bptree_node *cl, 
					bptree_node *cr)
{
	int l, r;
	assert(bpnode_is_leaf(cl) == bpnode_is_leaf(cr));
	if (!bpnode_is_leaf(cl)) return;

	// Two cases to handle:
	// Case 1: cursor is on the right node and we are about to move keys L
	if(bpnode_size(cl) < bpnode_size(cr) && 
		bpnode_is_same(cr, bps->cursor_node))
	{
		l = (int)ceil((bpnode_size(cl) + bpnode_size(cr)) / 2);
		r = (int)floor((bpnode_size(cl) + bpnode_size(cr)) / 2);
		int to_move = bpnode_size(cr) - r;
		bps->cursor_pos -= to_move;
		
		if (bps->cursor_pos < 0)
		{
			// We've moved the position to the other node
			bps->cursor_pos = bps->cursor_pos + l;
			cache_node_in_session(bps, cl);
			bps->cached_key_dirty = 1; 
		}
		
	}
	// Case 2: cursor is on the left node and we are about to move keys R
	else if (bpnode_size(cl) > bpnode_size(cr) &&
		bpnode_is_same(cl, bps->cursor_node))
	{
		l = (int)floor((bpnode_size(cl) + bpnode_size(cr)) / 2);
		r = (int)ceil((bpnode_size(cl) + bpnode_size(cr)) / 2);
		int to_move = bpnode_size(cl) - l;
		bps->cursor_pos += to_move;
		
		if (bps->cursor_pos >= l)
		{
			// We've moved the position to the other node
			bps->cursor_pos = to_move;
			cache_node_in_session(bps, cr);
			bps->cached_key_dirty = 1; 
		}
	}
	
	return;
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

/*@ Write out three nodes (a common situation) and return an error if any one
 * failed. TODO Figure out a *portable* way to use the stdargs.h header to make
 * this support any number of nodes (#include <stdargs.h> doesn't work on Fedora?!)
 */
int write_3_nodes(bptree_session *bps, bptree_node *n1, 
		bptree_node *n2, bptree_node *n3)
{

	if (write_node(bps, n1) != BPTREE_OP_SUCCESS)
		return BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT;
	if (write_node(bps, n2) != BPTREE_OP_SUCCESS)
		return BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT;
	if (write_node(bps, n3) != BPTREE_OP_SUCCESS)
		return BPTREE_OP_NODE_NOT_FOUND_OR_CORRUPT;
	
	return BPTREE_OP_SUCCESS;
}
// Wrapper for main compar() method but using key/val structs
int bptree_compar_keys(bptree_session *bps,
		const bptree_key_val *kv1, const bptree_key_val *kv2)
{
	return bptree_compar(bps, kv1->k, kv2->k, kv1->v, kv2->v,
			kv1->vsize,kv2->vsize, bps->num_fields);

}

// TODO Refactor
int bptree_compar_to_node(bptree_session *bps,
	bptree_node *x, const bptree_key_val *kv, int pos)
{
	char nb = '\0';
	int num_fields = num_fields_used(bps, kv);
	if (bps->insert_flags == BPTREE_INSERT_ALLOW_DUPES) {
		return bptree_compar(bps, bpnode_get_key(x,pos), kv->k, 
				     bpnode_get_value(x, pos), kv->v, 
				     bpnode_get_value_size(x, pos),kv->vsize, 
				     num_fields);
	}
	else 
	{
		return bptree_compar(bps, bpnode_get_key(x,pos), kv->k, 
				     &nb,&nb,1,1, num_fields);
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

void free_key_val(bptree_key_val **kv)
{
	if(*kv == NULL) assert (0 == 0xDEADBEEF);
	free((*kv)->k);
	free((*kv)->v);
	(*kv)->k = NULL;
	(*kv)->v = NULL;
	free(*kv);
	*kv = NULL;
}

// A generalization of the old compar function we used, but incorporating the
// information we have provided about what fields are present and their
// individual compar functions
int bptree_compar(bptree_session *bps, const void *k1, const void *k2,
		const void *v1, const void *v2, size_t vsize1, size_t vsize2, 
		int tot_fields)
{
	int i, res, offset;
	bptree_field *bf = bps->bfield;
	const unsigned char *a1 = k1;
	const unsigned char *a2 = k2;
	offset = 0;
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
	
	// Keypart is the same; return whether the values are the same
	if(bps->insert_flags == BPTREE_INSERT_UNIQUE_KEY)
	{
		return 0;
	} 
	else if (tot_fields == bps->num_fields)
	{
		if (vsize1 != vsize2) return (vsize1 < vsize2) ? -1 : 1;
		return memcmp(v1, v2, vsize1);
	}
	else
	{
		return res;
	}
}

static int bptree_update_recursive(bptree_session *bps,
		bptree_node* x, bptree_key_val *kv)
{
	int k_pos, c_pos, rv, rv_f ;

#ifdef DEFENSIVE_MODE
	assert(is_node_ordered(bps,x) == 0);
#endif

	rv_f = find_position_in_node(bps, x, kv, &k_pos, &c_pos);
	if (bpnode_is_leaf(x))
	{
		if (rv_f == BPTREE_OP_KEY_NOT_FOUND) return rv_f;
		
		clear_key_position(x,k_pos);
		bpnode_size_dec(x);
		copy_key_val_to_node(x, kv, k_pos);
		
		if(bpnode_is_same(x, bps->cursor_node) && 
			bps->cursor_pos >= k_pos)
		{
			bps->cached_key_dirty = 1;
		}
			
		return write_node(bps, x);
	}
	else
	{
		bptree_node *n;
		n = read_node(bps, bpnode_get_child_id(x,c_pos), &rv);
		if (rv != BPTREE_OP_NODE_FOUND) return rv;
		traversal_check(bps, x, n, c_pos);

		if (rv_f == BPTREE_OP_KEY_FOUND)
		{
			
			clear_key_position(x,k_pos);
			bpnode_size_dec(x);
			copy_key_val_to_node(x, kv, k_pos);
			rv = write_node(bps, x);
			if (rv != BPTREE_OP_SUCCESS) return rv;
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
	
	if (bps->insert_flags == BPTREE_INSERT_UNIQUE_KEY)
	{
		rv = bptree_search(bps, k, ksize, _val, &_vsize);
	}
	else if (bps->insert_flags == BPTREE_INSERT_ALLOW_DUPES)
	{
		rv = bptree_search_kv(bps, k, ksize, v, vsize);
	}
	
	// Search will set a cursor position, but we need to clear this since
	// it could lead to index_next being successfully called when it shouldn't
	clear_cursor(bps);

	kv.k = k;
	kv.v = v;
	kv.ksize = ksize;
	kv.vsize = vsize;
	
	if (rv == BPTREE_OP_KEY_FOUND ) return BPTREE_ERR_DUPLICATE_KEY_INSERTED;

	if(rv != BPTREE_OP_KEY_NOT_FOUND) return rv;

	rv = bptree_read_root(bps, &bpm, &root);
	if (rv != BPTREE_OP_NODE_FOUND) return rv;

	if (bpnode_is_full(root))
	{
		bptree_node *newroot = bpnode_new();
		bptree_node *root_sibling = bpnode_new();
		bpnode_set_leaf(newroot, 0);
		uuid_copy(bpm->root_key, bpnode_get_id(newroot));
		bpnode_set_parent(root, newroot);

		if (write_meta_node(bps, bpm, newroot) != BPTREE_OP_SUCCESS) 
			return BPTREE_OP_METADATA_ERROR;
		
		bpnode_set_child(newroot, 0, root);
		bpnode_set_child(newroot, 1, root_sibling);
		bptree_split_child(bps, newroot,0, root, root_sibling);
		rv = write_3_nodes(bps, newroot, root, root_sibling);
		if (rv != BPTREE_OP_SUCCESS) return rv;
		
		rv = bptree_insert_nonfull(bps, newroot, &kv, 1);

		assert(!bpnode_is_empty(newroot));
		//assert(uuid_compare(newroot->self_key, root->parent) == 0);
		//assert(uuid_compare(newroot->self_key, bpm->root_key) == 0);
		free_node(&newroot);
		free_node(&root_sibling);
	}
	else
	{
		rv = bptree_insert_nonfull(bps, root, &kv, 1);
	}
	free_node(&root);
	free_meta_node(&bpm);
	
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



/*@ Take a full node *cl and split it in two, promoting the middle key up to the
 * parent *p; *cr should be a fresh node provided by the caller
 */
void bptree_split_child(bptree_session *bps, bptree_node* p, int i, 
		       bptree_node* cl, bptree_node *cr)
{
	int j,rv;
	assert(bpnode_is_full(cl));
	assert(!bpnode_is_full(p));
#ifdef TRACE_MODE
	write_split_to_trace(bps, p, cl, NULL, i, lvl);
#endif
	if (bpnode_is_leaf(cl)) {
		bptree_split_child_leaf(bps, p, i, cl, cr);
	}
	else 
	{
		bptree_split_child_nonleaf(bps, p, i, cl, cr);
	}

	if(rv = are_split_cells_valid(bps, p, i, cl, cr) != 0) 
	{
		printf("ERROR: Bad split! rv %d\n", rv);
		dump_node_info(bps, p);
		dump_node_info(bps, cl);
		dump_node_info(bps, cr);
		assert(0);
	}
	
#ifdef TRACE_MODE
	write_split_to_trace(bps, x, y, n, i, lvl);
#endif
}


void bptree_split_child_leaf(bptree_session *bps, bptree_node* p, int i, 
		       bptree_node* cl, bptree_node *cr)
{
	int l,r,rv;
	assert(bpnode_is_leaf(cl));
	
	shift_bptree_node_children_right(p, i);
	shift_bptree_node_elements_right(p, i);
	
	l = BPTREE_MIN_DEGREE - 1;
	
	copy_bptree_node_element(cl, p, l, i);
	bpnode_size_inc(p);

	bpnode_set_leaf(cr, bpnode_is_leaf(cl));
	
	// Shift over elements into new node  - for leaf move also the splitkey
	for (r = 0; r < BPTREE_NODE_MIN_SIZE + 1; r++)
	{
		copy_bptree_node_element(cl, cr, l, r);
		clear_key_position(cl, l);
		bpnode_size_inc(cr);
		bpnode_size_dec(cl);
		l++;
	}

	bpnode_set_child(p, i+1, cr);
	bpnode_set_parent(cr, p);
	bpnode_set_parent(cl, p);

	bpnode_set_next_id(cr, bpnode_get_next_id(cl));
	bpnode_set_next(cl, cr);
	bpnode_set_prev(cr, cl);
}

void bptree_split_child_nonleaf(bptree_session *bps, bptree_node* p, int i, 
		       bptree_node* cl, bptree_node *cr)
{
	int l, r,rv;
	
	assert(!bpnode_is_leaf(cl));
	
	shift_bptree_node_children_right(p, i);
	shift_bptree_node_elements_right(p, i);
	
	l = BPTREE_MIN_DEGREE - 1;
	copy_bptree_node_element(cl, p, l, i);
	bpnode_set_child_id(cr, 0, bpnode_get_child_id(cl, l+1));
	bpnode_clear_child(cl, l+1);
	clear_key_position(cl, l);
	bpnode_size_dec(cl);
	bpnode_size_inc(p);
	l++; 

	bpnode_set_leaf(cr, bpnode_is_leaf(cl));
	// Shift over elements into new node 
	for (r = 0; r < BPTREE_NODE_MIN_SIZE ; r++)
	{
		copy_bptree_node_element(cl, cr, l, r);
		bpnode_size_inc(cr);
		bpnode_size_dec(cl);
		bpnode_set_child_id(cr, r+1, bpnode_get_child_id(cl, l+1));
		bpnode_clear_child(cl, l+1);
		clear_key_position(cl, l);
		l++;
	}

	bpnode_set_child(p, i+1, cr);
	bpnode_set_parent(cr, p);
	bpnode_set_parent(cl, p);
}

/*@ Finds the key and child position in a node of a key-value
 * (i.e. where the key is or would be, and what child should be traversed
 * to find it down the tree)
 */
int find_position_in_node(bptree_session *bps, bptree_node *x,
		bptree_key_val *kv, int *k_pos, int *c_pos)
{
	if (num_fields_used(bps, kv) == bps->num_fields)
	{
		return find_position_in_node_exact(bps, x, kv, k_pos, c_pos);
	}
	else
	{
		return find_position_in_node_partial(bps, x, kv, k_pos, c_pos);
	}
}

int find_position_in_node_partial(bptree_session *bps, bptree_node *x,
		bptree_key_val *kv, int *k_pos, int *c_pos)
{
	int i, cmp, prev, rv = BPTREE_OP_KEY_NOT_FOUND;
	if (bpnode_size(x) <= 0) 
	{
		*k_pos = 0;
		*c_pos = 0;
		return BPTREE_OP_KEY_NOT_FOUND;
	}

	cmp = prev = 1;
	for(i = bpnode_size(x)-1; i >= 0; i--)
	{
		cmp = bptree_compar_to_node(bps,x,kv,i);
		if (cmp < 0) break;
		prev = cmp;
	}
	i++;
	
	*k_pos = i;
	
	if (prev == 0) 
	{
		rv = BPTREE_OP_KEY_FOUND;
		//*c_pos = *k_pos + 1;
		*c_pos = *k_pos ;
	}
	else
	{
		rv = BPTREE_OP_KEY_NOT_FOUND;
		*c_pos = *k_pos;
	}	
	
	// Border case -- if kv is greater than the largest key, and ndoe is
	// full
	return rv;
}

int find_position_in_node_exact(bptree_session *bps, bptree_node *x,
		bptree_key_val *kv, int *k_pos, int *c_pos)
{
	int i, cmp, rv = BPTREE_OP_KEY_NOT_FOUND;
	if (bpnode_size(x) <= 0) 
	{
		*k_pos = 0;
		*c_pos = 0;
		return BPTREE_OP_KEY_NOT_FOUND;
	}

	for(i = 0; i < bpnode_size(x); i++)
	{
		cmp = bptree_compar_to_node(bps,x,kv,i);
		if (cmp >= 0) break;
	}
	
	*k_pos = i;
	
	if (cmp == 0) 
	{
		rv = BPTREE_OP_KEY_FOUND;
		*c_pos = *k_pos + 1;
	}
	else
	{
		*c_pos = *k_pos;
	}	
	
	return rv;
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

	int c_pos, k_pos, rv =BPTREE_OP_SUCCESS, rv2;

#ifdef TRACE_MODE
	write_insert_to_trace(bps, x, kv, pos, lvl);
#endif

	rv = find_position_in_node(bps, x, kv, &k_pos, &c_pos);
	
	if (bpnode_is_leaf(x))
	{
		if (bps->insert_flags == BPTREE_INSERT_UNIQUE_KEY && 
			rv == BPTREE_OP_KEY_FOUND)
		{
			return BPTREE_ERR_DUPLICATE_KEY_INSERTED;
		}

		if(bpnode_is_same(x, bps->cursor_node))
		{
			bps->cached_key_dirty = 1;
		}
		
		shift_bptree_node_elements_right(x,k_pos);
		copy_key_val_to_node(x, kv, k_pos);

		return write_node(bps, x);
	}
	else
	{
		bptree_node *n = NULL; 

		n = read_node(bps, bpnode_get_child_id(x,c_pos), &rv2);
		if (rv2 != BPTREE_OP_NODE_FOUND) return rv2;

		if (bpnode_is_full(n))
		{
			bptree_node *sp_node = bpnode_new();
			bptree_split_child(bps, x, c_pos, n, sp_node);
			rv2 = write_3_nodes(bps, x, n, sp_node);
			if (rv2 != BPTREE_OP_SUCCESS) return rv2;
			if(bptree_compar_to_node(bps, x, kv, c_pos) < 0) 
			{
				rv = bptree_insert_nonfull(bps, sp_node, kv, lvl+1);
			}
			else
			{
				rv = bptree_insert_nonfull(bps, n, kv, lvl+1);
			}
			free_node(&sp_node);
		}
		else
		{

			rv = bptree_insert_nonfull(bps, n, kv, lvl+1);
		}
		
		free_node(&n);
		return rv;
	}
}

int cache_node_in_session(bptree_session *bps, bptree_node *c)
{
	if (bps->cursor_node != NULL) free_node(&(bps->cursor_node));
	bps->cursor_node = copy_node(c);
	bps->cached_key_dirty = 0;
	bps->eof = 0;
}
static int bptree_search_recursive(bptree_session *bps,
		bptree_node* x, bptree_key_val *kv)
{
	int c_pos, k_pos, rv, rva;
	bptree_node *n;
#ifdef DEFENSIVE_MODE
	assert(is_node_ordered(bps,x) == 0);
#endif

	rv = find_position_in_node(bps, x, kv, &k_pos, &c_pos);
	if (bpnode_is_leaf(x))
	{
		
		cache_node_in_session(bps, x);
		bps->cursor_pos = k_pos;
		
		if(rv == BPTREE_OP_KEY_FOUND && bpnode_is_active(x, k_pos))
		{
			memcpy(kv->v, bpnode_get_value(x,k_pos), 
			       bpnode_get_value_size(x,k_pos));
			kv->vsize = bpnode_get_value_size(x,k_pos);
		}
		else
		{
			kv->vsize = 0;
		}
		return rv;
	}
	else
	{
		n = read_node(bps, bpnode_get_child_id(x,c_pos), &rv);
		if (rv != BPTREE_OP_NODE_FOUND)
		{
			kv->vsize = 0;
			return rv;
		}
		traversal_check(bps, x, n, c_pos);
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
		return BPTREE_OP_CURSOR_NOT_SET;
	}
	if (bps->cached_key_dirty)
	{
		bptree_node *c =
			read_node(bps, bpnode_get_id(bps->cursor_node), &rv);
		if (rv != BPTREE_OP_NODE_FOUND) return rv;
		cache_node_in_session(bps, c);
		free_node(&c);
	}

	bptree_key_val kv;
	kv.k = k;
	kv.v = v;
	rv = bptree_index_next_internal(bps, &kv);
	while (rv == BPTREE_OP_KEY_DELETED)
	{
		rv = bptree_index_next_internal(bps, &kv);
	}
	if (rv == BPTREE_OP_KEY_FOUND)
	{
		*ksize = kv.ksize;
		*vsize = kv.vsize;
	}
	else
	{
		*ksize = 0;
		*vsize = 0;
	}
	return rv;
}

int bptree_index_next_internal(bptree_session *bps, bptree_key_val *kv)
{
	int rv;

	if(bps->eof) bps->eof = 0;

#ifdef DEFENSIVE_MODE
	assert(bpnode_is_leaf(bps->cursor_node) == 1);
	assert(bps->cursor_pos >= 0 && bps->cursor_pos <= BPTREE_NODE_SIZE);
#endif

	while(!bpnode_is_active(bps->cursor_node, bps->cursor_pos) &&
		bps->cursor_pos < bpnode_size(bps->cursor_node)-1) bps->cursor_pos++;

	if (bps->cursor_pos >= bpnode_size(bps->cursor_node))
	{
		if(bpnode_is_eof(bps->cursor_node))
		{
			bps->eof = 1;
			bps->eof = 0;
			free_node(&(bps->cursor_node));
			return BPTREE_OP_EOF; // protect us from ourselves
		}
		else
		{
			// Read next cell
			bptree_node *next;
			next = read_node(bps, bpnode_get_next_id(bps->cursor_node), &rv);
			if (rv != BPTREE_OP_NODE_FOUND) {
				return rv;
			}
			bps->cursor_pos = 0;
			free_node(&(bps->cursor_node));
			bps->cursor_node = next;
		}

	}

	if(!bpnode_is_active(bps->cursor_node, bps->cursor_pos))
	{
		bps->cursor_pos++;
		return BPTREE_OP_KEY_DELETED;
	}

	bpnode_pop_kv(bps->cursor_node, bps->cursor_pos, kv);

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
	return BPTREE_OP_FAIL;
/* TODO Re-implement
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
*/
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
	bptree_key_val kv;

	rv = bptree_read_root(bps, &bpm, &root);
	if (rv != BPTREE_OP_NODE_FOUND)
	{
		*ksize = 0;
		*vsize = 0;
		return rv;
	}
	kv.k = k;
	kv.v = v;
	rv = bptree_index_first_recursive(bps, &kv, root);
	if (rv == BPTREE_OP_KEY_DELETED)
	{
		rv = bptree_index_next(bps,k, &kv.ksize, v, &kv.vsize);
	}
	if(rv == BPTREE_OP_KEY_FOUND)
	{
		*ksize = kv.ksize;
		*vsize = kv.vsize;
	}
	else
	{
		*ksize = 0;
		*vsize = 0;
	}
	free_node(&root);
	free_meta_node(&bpm);
	return rv;
}
static int bptree_index_first_recursive(bptree_session *bps,
					bptree_key_val *kv, bptree_node *n)
{
	bptree_node *next;
	int rv;
#ifdef DEFENSIVE_MODE
	assert(is_node_sane(n) == 0);
#endif
	if (bpnode_is_leaf(n))
	{
		if (bpnode_is_empty(n))
		{
			bps->eof = 1;
			//assert(bps->cursor_node == NULL); // nothing should have set
			return BPTREE_OP_EOF; // tree is empty
		}
		// In case our tree has only one element
		if (bpnode_size(n) == 1) bps->eof = 1;
		bpnode_pop_kv(n, 0, kv);
		bps->cursor_pos = 1;
		bps->cursor_node = copy_node(n);
		if(!bpnode_is_active(n, 0)) return BPTREE_OP_KEY_DELETED;
		return BPTREE_OP_KEY_FOUND;
	}
	else
	{
		next = read_node(bps, bpnode_get_child_id(n, 0), &rv);
		if (rv != BPTREE_OP_NODE_FOUND) return rv;
		rv = bptree_index_first_recursive(bps, kv, next);
		free_node(&next);
		return rv;
	}
}

int bptree_read_root(bptree_session *bps, bptree_meta_node **bpm,
		bptree_node **root)
{
	int rv;

	*bpm = read_meta_node(bps,&rv);
	if (rv != BPTREE_OP_NODE_FOUND) return rv;

	*root = read_node(bps, (*bpm)->root_key, &rv);
	if (rv != BPTREE_OP_NODE_FOUND) return rv;

	//assert(uuid_is_null((*root)->parent));
	assert(uuid_compare((*bpm)->root_key, bpnode_get_id(*root)) == 0);

	return rv;
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
		//assert(nsize >= sizeof(bptree_node));
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
	assert(is_node_sane(n) == 0);
	assert(is_node_ordered(bps,n) == 0);
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
	int rv, rva;
	void *k;
	void *v;
	key _k;
	val _v;
	unsigned char v_comp[BPTREE_MAX_VALUE_SIZE];

#ifdef DEFENSIVE_MODE
	rva = is_node_sane(n);
	if(rva != 0) {
		printf("Tried to write bad node, err %d\n", rva);
		dump_node_info(bps, n);
	}
	assert(rva == 0);
	rva = is_node_ordered(bps,n);
	if(rva != 0) {
		printf("Tried to write bad node, err %d\n", rva);
		dump_node_info(bps, n);
	}
	assert(rva == 0);
#endif

	k = create_cell_key(bps, bpnode_get_id(n), &ksize);
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
		//assert(uuid_compare(bpm->root_key, root->self_key) == 0);
		if (write_node(bps, root) != BPTREE_OP_SUCCESS) return BPTREE_OP_FAIL;
	}
#ifdef TRACE_MODE
	write_to_trace_file(0,&(bps->t->id), &_k, &_v, 0);
#endif
	free(k);
	free(v);
	return BPTREE_OP_SUCCESS;
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

// In searches, assert that the tree obeys the properties we expect
void assert_valid_parent_child_traversal(bptree_session *bps,
		bptree_node *p, bptree_node *c, int i)
{
	// If c is a leaf, then the first key of c should match p[i]
	bptree_key_val *kv_p, *kv_c;
	bpnode_get_kv_ref(p, i, kv_p);
	bpnode_get_kv_ref(c, i, kv_c);
	if (bpnode_is_leaf(c)) {
		if(bptree_compar_keys(bps, kv_p, kv_c) != 0) assert(0);
	}
	// if c is not a leaf, then c[0] should be strictly greater than p[i]
	else
	{
		if(bptree_compar_keys(bps, kv_p, kv_c) <= 0) assert(0);
	}
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

void free_meta_node(bptree_meta_node **m)
{
	free(*m);
	*m = NULL;
}


void traversal_check(bptree_session *bps, bptree_node *x, bptree_node *n,int i)
{
	int rv = is_valid_traversal(bps, x, n, i);
	if(rv != 0) {
		printf ("Caught what looks to be an invalid traversal!\n");
		dump_node_info(bps,x);
		printf("\nchild:\n");
		dump_node_info(bps,n);
	}
}

/*	 Similar to our split cells method;
 *  If we are traversing to a leaf node, the traversal key should be the same
 *   as the first key in the child, otherwise, it should be strictly greater
 *    Two other cases:
 *    i = 0 or i = bpnode_size(x) are boundary cases
	 */
int is_valid_traversal(bptree_session *bps, bptree_node *p, bptree_node *c,int i)
{
	int rv, pos;
	bptree_key_val p_kv, c_kv;
#ifndef DEFENSIVE_MODE
	return 0;
#endif

	if(bpnode_size(p) <= 0) return -1;

	if (i == 0)
	{
		// If this is a left hand traversal, the max value of n should be
		// strictly less than the first key of x
		pos = bpnode_size(c) -1;
		bpnode_get_kv_ref(p, 0, &p_kv);
		bpnode_get_kv_ref(c, pos, &c_kv);
		rv = bptree_compar_keys(bps, &p_kv, &c_kv);
		if (rv <= 0) return -2;
		else return 0;
	}
	else
	{
		pos = i-1;
		bpnode_get_kv_ref(p, pos, &p_kv);
		bpnode_get_kv_ref(c, 0, &c_kv);
		rv = bptree_compar_keys(bps, &p_kv, &c_kv);
	}
	
	if (bpnode_is_leaf(c) && rv > 0)
	{
		return -3;
	}
	else if (!bpnode_is_leaf(c) && rv >= 0)
	{
		return -5;
	}

	return 0;
}
	
int are_split_cells_valid(bptree_session *bps, bptree_node* p, int i,
		bptree_node *cl, bptree_node *cr)
{
#ifndef DEFENSIVE_MODE
	return 0;
#endif	
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

	 Check that the 
	 */

	int rv_l, rv_r;
	bptree_key_val p_kv, cl_kv, cr_kv;
	
	bpnode_get_kv_ref(p, i, &p_kv);
	bpnode_get_kv_ref(cl, bpnode_size(cl)-1, &cl_kv);
	bpnode_get_kv_ref(cr, 0, &cr_kv);
	
	rv_l = bptree_compar_keys(bps, &p_kv, &cl_kv);
	rv_r = bptree_compar_keys(bps, &p_kv, &cr_kv);

	if (rv_l < 0) return -1; // R strictly less than S in both cases above
	
	// Rewrite these checks as asserts
	if (bpnode_is_leaf(cl) && rv_r != 0) return -2; // S should be in root and new[0]
	if (!bpnode_is_leaf(cl) && rv_r >= 0) return -3; // i.e. T strictly less than S

	if(bpnode_size(cl) != BPTREE_MIN_DEGREE-1 ) return -4;
	if(bpnode_size(cr) != BPTREE_MIN_DEGREE -1 + bpnode_is_leaf(cr)) return -5;
	return 0;
}

int is_node_ordered(bptree_session *bps, bptree_node* y)
{
#ifndef DEFENSIVE_MODE
	return 0;
#endif	
	int i, rv;
	bptree_key_val kv1, kv2;
	for (i = 0; i < bpnode_size(y) - 1; i++)
	{
		bpnode_get_kv_ref(y, i, &kv1);
		bpnode_get_kv_ref(y, i+1, &kv2);
		rv = bptree_compar_keys(bps, &kv1, &kv2);
		if (rv >= 0) return (-1 * i); // if two key/values are the same we have a prob!
	}
	return 0;
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

/* Use this exclusively with CHAR fixed-length types in MySQL
We now no longer pass in the length because CHAR types are supposed to
be padded with spaces at the end */ 
inline int strncmp_mysql(const void *i1, const void *i2, size_t sz)
{
	// size_t len = sz - (sz > 255 ? 2 : 1);
	//const char *a = (const char*) i1 + (sz > 255 ? 2 : 1);
	//const char *b = (const char*) i2 + (sz > 255 ? 2 : 1);;
	return strncmp(i1,i2,sz);
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
