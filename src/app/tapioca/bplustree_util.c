#include "bplustree_util.h"
/***
 * Methods for (un)marshalling b+tree cell and header data to byte buffers */

void * marshall_bptree_meta_node(bptree_meta_node *bpm, size_t *bsize)
{
	tpl_node *tn;
	void *b;
	const char *tpl_fmt_str = BPTREE_TPL_META_NODE_FMT;
	 // iIv
	tn = tpl_map(tpl_fmt_str, &bpm->execution_id,
			&bpm->root_key, sizeof(uuid_t),
			&bpm->bpt_id);
	tpl_pack(tn, 0);
	tpl_dump(tn, TPL_MEM, &b, bsize);
	//    assert(*bsize == (sizeof(bptree_meta_node)));
	tpl_free(tn);
	return b;
}
bptree_meta_node * unmarshall_bptree_meta_node(const void *buf, size_t sz)
{
	int rv;
	tpl_node *tn;
	bptree_meta_node *bpm;
	if (buf == NULL) return NULL;

	const char *tpl_fmt_str = BPTREE_TPL_META_NODE_FMT;
#ifdef DEFENSIVE_MODE
	char *fmt = tpl_peek(TPL_MEM, buf, sz);
	if (fmt == NULL)
		return NULL;
	if (strcmp(tpl_fmt_str, fmt) != 0)
		return NULL;
	free(fmt);
#endif
	bpm = malloc(sizeof(bptree_meta_node));
	// iIv
	tn = tpl_map(tpl_fmt_str, &bpm->execution_id,
			&bpm->root_key, sizeof(uuid_t),
			&bpm->bpt_id);
	rv = tpl_load(tn, TPL_MEM | TPL_PREALLOCD | TPL_EXCESS_OK, buf,
			BPTREE_VALUE_SIZE);
	if (rv < 0) goto unmarshall_exception;
	rv = tpl_unpack(tn, 0);
	if (rv < 0) goto unmarshall_exception;
	tpl_free(tn);
	return bpm;

	unmarshall_exception: free(bpm);
	return NULL;
}

void * marshall_bptree_node(bptree_node *n, size_t *bsize)
{
	int i;
	tpl_node *tn;
	tpl_bin tb_keys;
	tpl_bin tb_values;
	void *b;
	const char *tpl_fmt_str = BPTREE_TPL_NODE_FMT;
	assert(is_node_sane(n));

	tn = tpl_map(tpl_fmt_str,
			&n->self_key, sizeof(uuid_t), // c#
			&n->key_count, &n->leaf, // ivv
			&n->key_sizes, BPTREE_NODE_SIZE, // i#
			&tb_keys, // A(B)
			&n->value_sizes, BPTREE_NODE_SIZE, // i#
			&tb_values, // A(B)
			&n->children, sizeof(uuid_t), BPTREE_NODE_SIZE+1, // c##
			&n->parent, sizeof(uuid_t),	// c#
			&n->prev_node, sizeof(uuid_t), // c#
			&n->next_node, sizeof(uuid_t), // c#
			&n->active, BPTREE_NODE_SIZE, // c#
			&n->tapioca_client_id,// j
			&n->write_count// i
			);

	tpl_pack(tn, 0); // pack the non-array elements?
	for (i = 0; i < BPTREE_NODE_SIZE; i++)
	{
		if (i >= n->key_count) break;
		tb_keys.addr = n->keys[i];
		tb_values.addr = n->values[i];
		tb_keys.sz = n->key_sizes[i];
		tb_values.sz = n->value_sizes[i];
		tpl_pack(tn, 1);
		tpl_pack(tn, 2);
	}
	tpl_dump(tn, TPL_MEM, &b, bsize);
	tpl_free(tn);
	return b;
}

bptree_node * unmarshall_bptree_node(const void *buf, size_t sz, size_t *nsize)
{
	int i, rv, rv1, rv2;
	tpl_node *tn;
	tpl_bin tb_keys;
	tpl_bin tb_values;
	bptree_node *n;
	//bptree_node n;
	const char *tpl_fmt_str = BPTREE_TPL_NODE_FMT;
	if (buf == NULL) return NULL;
#ifdef DEFENSIVE_MODE
	char *fmt = tpl_peek(TPL_MEM, buf, sz);
	if (fmt == NULL) return NULL;
	if (strcmp(tpl_fmt_str, fmt) != 0) return NULL;
	free(fmt);
#endif

	n = create_new_empty_bptree_node();
	*nsize = 0;
	if (n == NULL) return NULL;
	// Mapping is the same as with packing
	tn = tpl_map(tpl_fmt_str,
			&n->self_key, sizeof(uuid_t), // c#
			&n->key_count, &n->leaf, // ivv
			&n->key_sizes, BPTREE_NODE_SIZE, // i#
			&tb_keys, // A(B)
			&n->value_sizes, BPTREE_NODE_SIZE, // i#
			&tb_values, // A(B)
			&n->children, sizeof(uuid_t), BPTREE_NODE_SIZE+1, // c##
			&n->parent, sizeof(uuid_t),	// c#
			&n->prev_node, sizeof(uuid_t), // c#
			&n->next_node, sizeof(uuid_t), // c#
			&n->active, BPTREE_NODE_SIZE, // c#
			&n->tapioca_client_id, // j
			&n->write_count// i
			);

	*nsize += sizeof(bptree_node);
	rv = tpl_load(tn, TPL_MEM | TPL_PREALLOCD | TPL_EXCESS_OK, buf,
			BPTREE_VALUE_SIZE);
	if (rv == -1) goto unmarshall_exception;

	rv = tpl_unpack(tn, 0); // unpack the non-array elements.
	if (rv < 0) goto unmarshall_exception;
	for (i = 0; i < BPTREE_NODE_SIZE; i++)
	{
		if (i >= n->key_count) break;
		rv1 = tpl_unpack(tn, 1);
		rv2 = tpl_unpack(tn, 2);
		if (rv1 < 0 || rv2 < 0) return NULL;
		// TODO Defend against bad malloc
		if (tb_keys.sz > BPTREE_VALUE_SIZE || tb_values.sz > BPTREE_VALUE_SIZE)
			return NULL;
		n->keys[i] = malloc(tb_keys.sz);
		memcpy(n->keys[i], tb_keys.addr, tb_keys.sz);
		n->values[i] = malloc(tb_values.sz);
		memcpy(n->values[i], tb_values.addr, tb_values.sz);
		n->key_sizes[i] = tb_keys.sz;
		n->value_sizes[i] = tb_values.sz;
		*nsize += tb_keys.sz + tb_values.sz;
		free(tb_values.addr);
		free(tb_keys.addr);
	}
	tpl_free(tn);

	return n;

	unmarshall_exception: free(n);
	return NULL;
	//return 1;
}


bptree_node * create_new_empty_bptree_node()
{
	int i;
	bptree_node *n = malloc(sizeof(bptree_node));
	if (n == NULL) return NULL;
	n->key_count = 0;
	n->leaf = 1;
	n->tapioca_client_id = -1;
	n->write_count = 0;

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
/** Copy all memory associated with node and return a new pointer */
// TODO The probability that i implemented this correctly on teh first try
// are not good. Double check.
bptree_node *copy_node(bptree_node *n)
{
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
	x->write_count = n->write_count;
	x->last_version = n->last_version;
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
