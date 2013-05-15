/*@
 * Code that is not core to B+Tree functionality used to do things like
 * dump the tree to a file, verify its contents, etc.
 */

#include "bplustree.h"
#include <stdio.h>
#include <math.h>


//int bptree_sequential_read_recursive(bptree_session *bps, tapioca_bptree_id *tbpt_id,
//		bptree_node *n, int binary);
//int output_bptree_recursive(bptree_session *bps, tapioca_bptree_id *tbpt_id,
//		bptree_node* n, int level, int binary);

//int dump_bptree_contents(bptree_session *bps, tapioca_bptree_id tbpt_id, int dbug,
//		int binary);
int output_bptree_recursive(bptree_session *bps,bptree_node* n,
		int level, FILE *fp);

int dump_bptree_sequential(bptree_session *bps, uuid_t failed_node);
int verify_bptree_sequential(bptree_session *bps, uuid_t failed_node);
bptree_key_val *
	verify_bptree_recursive_read(bptree_session *bps,bptree_node *n, int *rv);

//int verify_bptree_order_recursive(bptree_session *bps, tapioca_bptree_id *tbpt_id,
//		bptree_node *n, char *largest);
int bptree_debug(bptree_session *bps, enum bptree_debug_option debug_opt,
		void *data);

int bptree_index_scan(bptree_session *bps,bptree_node *root, uuid_t failed_node);
int bptree_index_scan_recursive(bptree_session *bps, bptree_node *n,
		bptree_key_val *prune_kv, uuid_t failed_node, int *nodes);
int bptree_get_key_length(bptree_session *bps);


int bptree_debug(bptree_session *bps, enum bptree_debug_option debug_opt,
		void *data)
{
	int rv;
	bptree_meta_node *bpm;
	bptree_node *root;
	bptree_key_val *kvmax;
	char s1[512];
	memset(&kvmax,'\0',sizeof(bptree_key_val));

	rv = bptree_read_root(bps, &bpm, &root);
	if (rv != BPTREE_OP_NODE_FOUND) return rv;

	switch (debug_opt) {
		case BPTREE_DEBUG_VERIFY_SEQUENTIALLY:
			rv = verify_bptree_sequential(bps, data);
			if(rv == BPTREE_OP_SUCCESS)
			{
				fflush(stdout);
				bps->eof = 0;
			}
			break;
		case BPTREE_DEBUG_VERIFY_RECURSIVELY:
			kvmax = verify_bptree_recursive_read(bps,root,&rv);
			if (rv == BPTREE_OP_SUCCESS) {
				bptree_key_value_to_string_kv(bps, kvmax, s1);
				printf("Largest key found: %s \n",s1);
				fflush(stdout);
				bps->eof = 0;
			} else if(rv == 0) {
				printf("Recursive check failed!\n");
				fflush(stdout);
			}
			break;
		case BPTREE_DEBUG_DUMP_SEQUENTIAL:
			rv = dump_bptree_sequential(bps, data);
			break;
		case BPTREE_DEBUG_DUMP_RECURSIVE:
			rv = output_bptree(bps,1);
			if(rv == BPTREE_OP_SUCCESS) fflush(stdout);
			break;
		case BPTREE_DEBUG_INDEX_RECURSIVE_SCAN:
//			printf("Doing recursive scan of index\n");
			rv = bptree_index_scan(bps, root, data);
			if(rv == BPTREE_OP_SUCCESS) bps->eof = 0;
			break;
		default:
			break;
	}

	return rv;
}

void bptree_key_value_to_string_kv(bptree_session *bps, bptree_key_val *kv,
		char *out)
{
	bptree_key_value_to_string(bps,kv->k,kv->v,kv->ksize,kv->vsize, out);
}
/*@
 * Based on the field configuration of this bptree, interpret the key/value
 * as a string suitable for printing
 * Assumes the string provided is at least 512 chars long!
 * TODO Make this method somewhat less vulnerable
 */
void bptree_key_value_to_string(bptree_session *bps, unsigned char *k,
		unsigned char *v, int32_t ksize, int32_t vsize, char *out)
{

	int8_t i8; int16_t i16; int32_t i32; int64_t i64; char *s;
	const int BUF_LEN = 512;
	char buf[BUF_LEN];
	char *o = out;
	int i, offset = 0, len = 0, chars;
	bptree_field *bf = bps->bfield;
	bzero(out,BUF_LEN);

	sprintf(buf, "( ");
	strncpy(o, buf, 2);
	o+=2;
	for (i = 1; i <= bps->num_fields; i++)
	{
		bzero(buf,BUF_LEN);
		switch (bf->field_type) {
			case BPTREE_FIELD_COMP_INT_8:
				memcpy(&i8,(k+offset),sizeof(int8_t));
				chars = sprintf(buf, "%d",i8);
				break;
			case BPTREE_FIELD_COMP_INT_16:
				memcpy(&i16,(k+offset),sizeof(int16_t));
				chars = sprintf(buf, "%d",i16);
				break;
			case BPTREE_FIELD_COMP_INT_32:
				memcpy(&i32,(k+offset),sizeof(int32_t));
				chars = sprintf(buf, "%d",i32);
				break;
			case BPTREE_FIELD_COMP_INT_64:
				memcpy(&i64,(k+offset),sizeof(int64_t));
				chars = sprintf(buf, "%jd",i64);
				break;
			case BPTREE_FIELD_COMP_MYSQL_STRNCMP:
				memcpy(&i8, (k+offset), sizeof(int8_t));
				s = (char *)(k+offset+1); // Skip past the size param
				chars = bf->f_sz - 1;
				memset(buf, '\20', chars);
				strncpy(buf, s, chars);
				break;
			case BPTREE_FIELD_COMP_STRNCMP:
				s = (char *) (k+offset);
				chars = bf->f_sz;
				memset(buf, '\20', chars);
				strncpy(buf, s, chars);
				break;
			case BPTREE_FIELD_COMP_MEMCMP:
				sprintf("%x",(char *)(k+offset));
				break;
			default:
				break;
		}

		len += chars;
		if (len > BUF_LEN) break;
		strncpy(o, buf, chars);
		o += chars;
		offset += bf->f_sz;
		bf++;
		if(i < bps->num_fields)
		{
			len += sprintf(buf, ", ");
			strncpy(o, buf, 2);
			o+=2;
		}
	}
	sprintf(buf, " )");
	strncpy(o, buf, 2);
	o+=2;
	*o = '\0';

//	return out;
}

int bptree_get_key_length(bptree_session *bps)
{
	int i, sz = 0;
	bptree_field *bf = bps->bfield;
	for (i = 0; i < bps->num_fields; i++)
	{
		sz+= bf->f_sz;
	}
	return sz;

}

// TODO Something in this function is messing up cursor positioning for a
// successive call to bptree_first
int verify_bptree_sequential(bptree_session *bps, uuid_t failed_node)
{
	int ksize_p, vsize_p, ksize_c, vsize_c, rv;
	const int BUF_SZ = 1024;
	unsigned char kcur[BUF_SZ], kprev[BUF_SZ], vcur[BUF_SZ], vprev[BUF_SZ];
	char s1[512], s2[512];
	int elements = 1;
	bzero(kcur, BUF_SZ);
	bzero(vcur, BUF_SZ);
	bzero(kprev, BUF_SZ);
	bzero(vprev, BUF_SZ);

	if (!uuid_is_null(failed_node))
	{
//		uuid_copy(bps->cursor_cell_id, failed_node);
		bps->cursor_node = read_node(bps, failed_node, &rv);
		if(rv == BPTREE_OP_TAPIOCA_NOT_READY) return rv;
		bps->cursor_pos = 0;

		rv = bptree_index_next(bps, kprev, &ksize_p, vprev, &vsize_p);
//		if (rv == BPTREE_OP_TAPIOCA_NOT_READY)
//			uuid_copy(failed_node, bps->cursor_node->self_key);
		if (rv != BPTREE_OP_KEY_FOUND) return rv;
	}
	else
	{
		rv = bptree_index_first(bps, kprev, &ksize_p, vprev, &vsize_p);
	}
	if (rv < 0) return rv;
	if (rv == BPTREE_OP_TAPIOCA_NOT_READY) return BPTREE_OP_TAPIOCA_NOT_READY;

	for(rv =0;;)
	{
		rv = bptree_index_next(bps, kcur, &ksize_c, vcur, &vsize_c);
		if (rv != BPTREE_OP_KEY_FOUND) break;
		elements++;
		if (bptree_compar(bps,kprev, kcur, vprev, vcur, vsize_p, vsize_c) > 0)
		{
			bptree_key_value_to_string(bps,kprev,vprev,ksize_p,vsize_p,s1);
			bptree_key_value_to_string(bps,kcur,vcur,ksize_c,vsize_c,s2);
			printf("B+Tree validation failed, %s > %s\n", s1, s2);
			fflush(stdout);
			return BPTREE_OP_TREE_ERROR;
		}
		bzero(kprev, BUF_SZ);
		bzero(vprev, BUF_SZ);
		memcpy(kprev, kcur, BUF_SZ);
		memcpy(vprev, vcur, BUF_SZ);
		bzero(kcur, BUF_SZ);
		bzero(vcur, BUF_SZ);
	}
	if (rv == BPTREE_OP_TAPIOCA_NOT_READY)
	{
		// TODO Rethink this - will probably segfault on multi-node
		uuid_copy(failed_node, bps->cursor_node->self_key);
	}
	printf("Sequential scan returned %d elements\n", elements);
	return BPTREE_OP_SUCCESS;

}


int dump_bptree_sequential(bptree_session *bps, uuid_t failed_node)
{
	int ksize, vsize, rv;
	unsigned char k[BPTREE_MAX_VALUE_SIZE], v[BPTREE_MAX_VALUE_SIZE];
	char uuid_out[40];
	char s1[512];
	printf("Dumping bpt_id %d:\n",bps->bpt_id);
	fflush(stdout);
	if (!uuid_is_null(failed_node))
	{
		bps->cursor_node = read_node(bps, failed_node, &rv);
		if(rv == BPTREE_OP_TAPIOCA_NOT_READY) return rv;
		bps->cursor_pos = 0;
		rv = bptree_index_next(bps, k, &ksize, v, &vsize);
		if (rv != BPTREE_OP_KEY_FOUND) return rv;
	}
	else
	{
		bptree_index_first(bps, k, &ksize, v, &vsize);
	}

	for(rv = 0;;)
	{
		bptree_key_value_to_string(bps, k,v,ksize,vsize,s1);
		uuid_unparse(bps->cursor_node->self_key,uuid_out);
		printf("Node->Cell %s -> %d \t Key: %s \n",
				uuid_out, bps->cursor_pos, s1);
		rv = bptree_index_next(bps, k, &ksize, v, &vsize);
		if (rv != BPTREE_OP_KEY_FOUND) break;
	}
	if (rv == BPTREE_OP_EOF)
	{
		printf("\n\n");
		fflush(stdout);
	}
	else if (rv == BPTREE_OP_TAPIOCA_NOT_READY)
	{
		uuid_copy(failed_node, bps->cursor_node->self_key);
	}
	return rv;
}

/*
 * Returns the largest value found in the subtree into kvlg;
 */
//int
bptree_key_val *
verify_bptree_recursive_read(bptree_session *bps,bptree_node *n,
		int *rv)
{
	int c, rv2, invalid = 1;
	bptree_key_val loc_max;
	char s1[512], s2[512];
	bptree_key_val *subtree_max = malloc(sizeof(bptree_key_val));

	get_key_val_from_node(n,n->key_count-1, &loc_max);

	if (!is_cell_ordered(bps, n))
	{
		printf("Bpt %d cell %ld was not ordered!\n", bps->bpt_id, n->self_key);
		*rv = 0;
		return NULL;
	}

	if (!n->leaf)
	{
		for (c = 0; c <= n->key_count; c++)
		{
			bptree_node *child;
			bptree_key_val *kv;
			child = read_node(bps, n->children[c], &rv2);
			if (rv2 != BPTREE_OP_NODE_FOUND)
			{
				*rv = rv2;
				return NULL;
			}
			assert_parent_child(bps, n, child );
			kv = verify_bptree_recursive_read(bps, child, &rv2);
			if (rv2 != BPTREE_OP_SUCCESS)
			{
				*rv = rv2;
				return NULL;
			}

			// The max key in this node should be greater than all child maxes
			// other than the final node
			invalid = 0;
			if (c < n->key_count && bptree_compar_keys(bps, kv, &loc_max) > 0)
			{
				invalid = 1;
			}
			else if (c>= n->key_count)
			{
				if(bptree_compar_keys(bps,kv,&loc_max) < 0) invalid = 2;
				copy_key_val(subtree_max, kv);
			}

			if (invalid)
			{
				bptree_key_value_to_string_kv(bps,kv, s1);
				bptree_key_value_to_string_kv(bps,&loc_max, s2),
				printf("B+Tree recursive validation failed, %s , %s, inv %d\n",
						s1, s2, invalid);
				fflush(stdout);
				*rv = 0;
				return NULL;
			}
			free_key_val(&kv);
			free_node(&child);
		}
	}
	else {
		copy_key_val(subtree_max, &loc_max);
	}
	*rv = BPTREE_OP_SUCCESS;
	return subtree_max;
}


/*@ Procedure to read all nodes of a particular btree, but not do
 * anything in particular with them; the primary use of this is to ensure that
 * as much (usually all, if mem available) of the btree node keys become
 * cached on the node this is run from; this is useful for verification but also
 * for improving performance on cache nodes for certain tables
 * The procedure can restart from the last node it failed to read from on
 * multi-node configurations
 */
int bptree_index_scan(bptree_session *bps,bptree_node *root, uuid_t failed_node)
{
	int rv;
	int nodes =0;
	bptree_node *n;
	bptree_key_val kv;
	char uuid_out[40];
	if (!uuid_is_null(failed_node))
	{
		n = read_node(bps, failed_node, &rv);
		if (rv != BPTREE_OP_NODE_FOUND) return rv;
		uuid_unparse(failed_node, uuid_out);
		printf("Previous failed node %s \n", uuid_out);
		get_key_val_from_node(n,0,&kv);

	} else {
		// Use a 'null' key as our comparison point to grab everything
		// FIXME Implement proper size function here
		kv.ksize = bptree_get_key_length(bps);
		kv.vsize = 4;
		kv.k = malloc(kv.ksize);
		kv.v = malloc(kv.vsize);
		memset(kv.k,'\0',kv.ksize);
		memset(kv.v,'\0',kv.vsize);
	}
	rv = bptree_index_scan_recursive(bps, root, &kv, failed_node, &nodes);
	if (rv == BPTREE_OP_SUCCESS)
	{
		printf ("Scanned %d nodes \n",nodes);
		fflush(stdout);
	}
	return rv;
}
// Scan only values larger than *prune_kv
int bptree_index_scan_recursive(bptree_session *bps, bptree_node *n,
		bptree_key_val *prune_kv, uuid_t failed_node, int *nodes)
{
	int c, rv;
	bptree_key_val kv;
	char uuid_out[40];

	if (!is_cell_ordered(bps, n) || !is_bptree_node_sane(n)
			|| !are_key_and_value_sizes_valid(n))
	{
		printf("BTree node not sorted correctly or invalid!\n");
		return BPTREE_OP_TREE_ERROR;
	}
	if (!n->leaf)
	{
		for (c = 0; c <= n->key_count; c++)
		{
			bptree_node *child;

			if (c < n->key_count) get_key_val_from_node(n, c, &kv);

			if (bptree_compar_keys(bps, &kv, prune_kv) > 0 )
			{
				child = read_node(bps, n->children[c], &rv);
				if (rv != BPTREE_OP_NODE_FOUND)
				{
					uuid_copy(failed_node, n->children[c]);
					uuid_unparse(failed_node, uuid_out);
					printf("Failed on node %s after %d reading nodes\n",
							uuid_out, *nodes);
					return rv;
				}
				if(uuid_compare(n->self_key, child->parent) != 0)
				{
					printf("Misalignment of parent-child in tree!\n");
					return BPTREE_OP_TREE_ERROR;
				}

				(*nodes)++;
				rv= bptree_index_scan_recursive(bps,child,
						prune_kv,failed_node,nodes);

				free_node(&child);

				if (rv != BPTREE_OP_SUCCESS) return rv;
			}
		}
	}
	return BPTREE_OP_SUCCESS;
}


int output_bptree(bptree_session *bps, int i ) {
        int rv;
    bptree_node *root;
    bptree_meta_node *bpm;
    char path[60];
	char uuid_out[40];
	char uuid_upper[9];
	bzero(uuid_upper,9);
	rv = bptree_read_root(bps, &bpm, &root);
	if (rv != BPTREE_OP_NODE_FOUND) return rv;

    sprintf(path, "/tmp/%d-%d.dot", bps->bpt_id, i);
    FILE *fp = fopen(path,"w");
    fprintf(fp, "\ndigraph BTree { \n");
	uuid_unparse(bpm->root_key, uuid_out);
	strncpy(uuid_upper, uuid_out, 8);
    fprintf(fp, "META [ label = < %s > ]", uuid_upper);
    fprintf(fp, "META -> \"N-%s\"\n [color=\"#00CC00\"] ", uuid_out);
    rv = output_bptree_recursive(bps, root, 0, fp);
    fprintf(fp, "\n\n } \n\n");

    fclose(fp);
    return rv;
}

int output_bptree_recursive(bptree_session *bps,bptree_node* n,
		int level, FILE *fp) {
	int k,c, rv;
	char uuid_out_n[40], uuid_out_c[40];
	char uuid_upper[9];
	char s1[512], s2[512];
	bptree_key_val kv;
	char *key_str;
	assert (n->key_count <= 2*BPTREE_MIN_DEGREE-1);
	uuid_unparse(n->self_key, uuid_out_n);
	fprintf(fp, "\n\n");
	fprintf(fp, "\"N-%s\" [ label = < ", uuid_out_n);
	// use the first 8 chars of the uuid as the label
	strncpy(uuid_upper, uuid_out_n, 8);
	char ch = 'I';
	if(n->leaf) ch = 'L';
	uuid_upper[8] = '\0';
//	fprintf(fp, "%s (%d) [%d] %c :  ", uuid_upper, n->key_count, n->tapioca_client_id, ch);
	if (n->key_count > 0) {
		get_key_val_from_node(n, 0, &kv);
		bptree_key_value_to_string_kv(bps, &kv, s1);
		//	free(key_str);
		fprintf(fp, "  %s  ", s1);
		get_key_val_from_node(n, n->key_count-1, &kv);
		bptree_key_value_to_string_kv(bps, &kv, s2);
		//	free(key_str);
		fprintf(fp, " %s ", s2);

	}
	fprintf(fp, " > ");
	if (uuid_is_null(n->parent)) fprintf(fp, " , shape=\"diamond\"");
	else if (!n->leaf) fprintf(fp, " , shape=\"rect\"");
	fprintf(fp, " ] \n");
	if (!n->leaf) {
		for (c = 0; c <= n->key_count; c++) {
			uuid_unparse(n->children[c], uuid_out_c);
			fprintf(fp, "\"N-%s\" -> \"N-%s\"\n [color=\"#CC0000\"] ",
					uuid_out_n, uuid_out_c);
		}
		for (c = 0; c <= n->key_count; c++) {
			bptree_node *next;
			next = read_node(bps,n->children[c], &rv);
			if ( next == NULL) return -1;
			output_bptree_recursive(bps, next, level+1, fp);
		}
	} else {
		uuid_unparse(n->next_node, uuid_out_c);
//		if (!uuid_is_null(n->next_node))
//			fprintf(fp, "\"N-%s\" -> \"N-%s\"\n ", uuid_out_n, uuid_out_c);
//		if (!uuid_is_null(n->prev_node))
//			uuid_unparse(n->prev_node, uuid_out_c);
//		fprintf(fp, "\"N-%s\" -> \"N-%s\" [weight = 100.0]\n ", uuid_out_n, uuid_out_c);
	}
	// Print parent link
	uuid_unparse(n->parent, uuid_out_c);
	if (!uuid_is_null(n->parent))
		fprintf(fp, "\"N-%s\" -> \"N-%s\" [color=\"#CC0000\"] \n ",
			uuid_out_n, uuid_out_c);

	return BPTREE_OP_SUCCESS;
}
