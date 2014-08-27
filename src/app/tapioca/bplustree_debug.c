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

/*@
 * Code that is not core to B+Tree functionality used to do things like
 * dump the tree to a file, verify its contents, etc.
 */

#include "bplustree.h"
#include "bptree_node.h"
#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <stdio.h>


int output_bptree_recursive(bptree_session *bps,bptree_node* n,
		int level, FILE *fp);

int dump_bptree_sequential(bptree_session *bps, uuid_t failed_node);
int verify_bptree_sequential(bptree_session *bps, uuid_t failed_node);
bptree_key_val *
	verify_bptree_recursive_read(bptree_session* bps, bptree_node* n, int dump_to_text, FILE* fp, int level, int* rv);

int bptree_index_scan(bptree_session *bps,bptree_node *root, uuid_t failed_node,
	FILE *fp);
int bptree_index_scan_recursive(bptree_session *bps, bptree_node *n,
		bptree_key_val *prune_kv, uuid_t failed_node, int *nodes, 
		FILE *fp);
int bptree_get_key_length(bptree_session *bps);


int bptree_debug(bptree_session *bps, enum bptree_debug_option debug_opt,
		void *data)
{
	int rv;
	bptree_meta_node *bpm;
	bptree_node *root;
	bptree_key_val *kvmax;
	char s1[512];
	FILE *fp;
	memset(&kvmax,'\0',sizeof(bptree_key_val));

	rv = bptree_read_root(bps, &bpm, &root);
	if (rv != BPTREE_OP_NODE_FOUND) return rv;
	
	if (bpnode_is_leaf(root)  && bpnode_is_empty(root))
	{
		// Btree is empty
		return BPTREE_OP_SUCCESS;
	}

	switch (debug_opt) {
		case BPTREE_DEBUG_VERIFY_SEQUENTIALLY:
			rv = verify_bptree_sequential(bps, data);
			if(rv == BPTREE_OP_SUCCESS)
			{
				fflush(stdout);
				bps->eof = 0;
			}
			break;
		case BPTREE_DEBUG_DUMP_RECURSIVELY:
			sprintf(s1, "/tmp/%d-recursive.out", bps->bpt_id);
			fp = fopen(s1,"w");
			kvmax = verify_bptree_recursive_read(bps,root,1, fp,0, &rv);
			if (rv == BPTREE_OP_SUCCESS) {
				bptree_key_value_to_string_kv(bps, kvmax, s1);
				printf("Largest key found: %s \n",s1);
				fflush(stdout);
				bps->eof = 0;
			} else if(rv == 0) {
				printf("Recursive check failed!\n");
				fflush(stdout);
			}
			fflush(fp);
			fclose(fp);
			break;
		case BPTREE_DEBUG_DUMP_NODE_DETAILS:
			sprintf(s1, "/tmp/%d-nodedetails.out", bps->bpt_id);
			fp = fopen(s1,"w");
			rv = bptree_index_scan(bps, root, data, fp);
			if(rv == BPTREE_OP_SUCCESS) bps->eof = 0;
			fflush(fp);
			fclose(fp);
			break;
		case BPTREE_DEBUG_VERIFY_RECURSIVELY:
			kvmax = verify_bptree_recursive_read(bps,root,0, NULL,0, &rv);
			if (rv == BPTREE_OP_SUCCESS) {
				fflush(stdout);
				bps->eof = 0;
			} else if(rv == 0) {
				printf("Recursive check failed!\n");
				fflush(stdout);
			}
			break;
		case BPTREE_DEBUG_DUMP_SEQUENTIALLY:
			rv = dump_bptree_sequential(bps, data);
			break;
		case BPTREE_DEBUG_DUMP_GRAPHVIZ:
			rv = output_bptree(bps,1);
			if(rv == BPTREE_OP_SUCCESS) fflush(stdout);
			break;
		case BPTREE_DEBUG_INDEX_RECURSIVE_SCAN:
			rv = bptree_index_scan(bps, root, data, NULL);
			if(rv == BPTREE_OP_SUCCESS) bps->eof = 0;
			break;
		default:
			break;
	}

	if (rv != BPTREE_OP_SUCCESS) 
		printf ("Debug type %d failed with code %d\n",debug_opt,rv);
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
				chars = sprintf(buf, "%x",(char *)(k+offset));
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
		if (bptree_compar(bps,kprev, kcur, vprev, vcur, vsize_p, vsize_c, 
			bps->num_fields) > 0)
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
		uuid_copy(failed_node, bpnode_get_id(bps->cursor_node));
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
	char path[128];
    sprintf(path, "/tmp/%d.out", bps->bpt_id);
    FILE *fp = fopen(path,"w");
	//printf("Dumping bpt_id %d:\n",bps->bpt_id);
	//fflush(stdout);
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
		uuid_unparse(bpnode_get_id(bps->cursor_node), uuid_out);
		fprintf(fp, "Node->Cell %s -> %d \t Key: %s \n",
				uuid_out, bps->cursor_pos, s1);
		rv = bptree_index_next(bps, k, &ksize, v, &vsize);
		if (rv != BPTREE_OP_KEY_FOUND) break;
	}
	if (rv == BPTREE_OP_EOF)
	{
		fprintf(fp, "\n\n");
		fflush(stdout);
		rv = BPTREE_OP_SUCCESS;
	}
	else if (rv == BPTREE_OP_TAPIOCA_NOT_READY)
	{
		uuid_copy(failed_node, bpnode_get_id(bps->cursor_node));
		//uuid_copy(failed_node, bps->cursor_node->self_key);
	}
	return rv;
	fflush(fp);
	fclose(fp);
}

/*
 * Returns the largest value found in the subtree into kvlg;
 */
//int
bptree_key_val *
verify_bptree_recursive_read(bptree_session *bps,bptree_node *n,
		int dump_to_text, FILE *fp, int level, int *rv)
{
	int c, rv2, invalid = 1;
	bptree_key_val loc_max;
	char s1[512], s2[512], uuid_out[40];
	bptree_key_val *subtree_max = malloc(sizeof(bptree_key_val));

	bpnode_get_kv_ref(n, bpnode_size(n) - 1, &loc_max);

	if (is_node_ordered(bps, n) != 0)
	{
		printf("Bpt %d cell %ld was not ordered!\n", 
		       bps->bpt_id, bpnode_get_id(n));
		*rv = 0;
		return NULL;
	}

	if (!bpnode_is_leaf(n))
	{
		for (c = 0; c <= bpnode_size(n); c++)
		{
			bptree_node *child;
			bptree_key_val *kv;
			child = read_node(bps,bpnode_get_child_id(n, c), &rv2);
			if (rv2 != BPTREE_OP_NODE_FOUND)
			{
				*rv = rv2;
				return NULL;
			}
			if (dump_to_text && c < bpnode_size(n))
			{
				int i;
				bptree_key_val kv_out;
				bpnode_get_kv_ref(n,c, &kv_out);
				bptree_key_value_to_string_kv(bps, &kv_out, s1);
				uuid_unparse(bpnode_get_id(n),uuid_out);
				fprintf(fp,"%d",level);
				for (i = 0 ; i<level; i++) fprintf(fp, "-");
				fprintf(fp, " Node->Cell %s -> %d \t Key: %s \t chld_sz %d \n",
						uuid_out, c, s1, bpnode_size(child));
			}
			kv = verify_bptree_recursive_read(bps, child, dump_to_text,fp, level+1, &rv2);
			if (rv2 != BPTREE_OP_SUCCESS)
			{
				*rv = rv2;
				return NULL;
			}

			// The max key in this node should be greater than all child maxes
			// other than the final node
			invalid = 0;
			if (c < bpnode_size(n) && bptree_compar_keys(bps, kv, &loc_max) > 0)
			{
				invalid = 1;
			}
			else if (c>= bpnode_size(n))
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
		if (dump_to_text)
		{
			int c;
			for (c = 0; c < bpnode_size(n); c++) {
				int i;
				bptree_key_val kv_out;
				bpnode_get_kv_ref(n,c, &kv_out);
				bptree_key_value_to_string_kv(bps, &kv_out, s1);
				uuid_unparse(bpnode_get_id(n),uuid_out);
				fprintf(fp,"%d",level);
				for (i = 0 ; i<level; i++) fprintf(fp, "-");
				fprintf(fp, " Node->Cell %s -> %d \t Key: %s \t chld_sz %d \n",
						uuid_out, c, s1, 0);
			}
			// Don't dump the leaf nodes for now, we have them sequentially */
			//bptree_key_value_to_string_kv(bps, loc, s1);
			//uuid_unparse(bps->cursor_node->self_key,uuid_out);
			//fprintf(fp, "Leaf  Node->Cell %s -> %d \t Key: %s \n",
					//uuid_out, bps->cursor_pos, s1);
		}
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
int bptree_index_scan(bptree_session *bps,bptree_node *root, uuid_t failed_node,
	FILE *fp)
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
		bpnode_get_kv_ref(n,0,&kv);

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
	rv = bptree_index_scan_recursive(bps, root, &kv, failed_node, &nodes, fp);
	if (rv == BPTREE_OP_SUCCESS)
	{
		fflush(stdout);
	}
	return rv;
}
// Scan only values larger than *prune_kv
int bptree_index_scan_recursive(bptree_session *bps, bptree_node *n,
		bptree_key_val *prune_kv, uuid_t failed_node, int *nodes,
		FILE *fp)
{
	int c, rv;
	bptree_key_val kv;
	char uuid_out[40];

	if (is_node_ordered(bps, n) != 0)
	{
		printf("BTree node not sorted correctly or invalid!\n");
		return BPTREE_OP_TREE_ERROR;
	}

	if (fp != NULL) 
	{
		dump_node_info_fp(bps, n, fp);
	}
	
	if (!bpnode_is_leaf(n))
	{
		for (c = 0; c <= bpnode_size(n); c++)
		{
			bptree_node *child;

			if (c < bpnode_size(n)) bpnode_get_kv_ref(n, c, &kv);

			if (bptree_compar_keys(bps, &kv, prune_kv) > 0 )
			{
				child = read_node(bps, bpnode_get_child_id(n, c), &rv);
				if (rv != BPTREE_OP_NODE_FOUND)
				{
					uuid_copy(failed_node, bpnode_get_child_id(n, c));
					uuid_unparse(failed_node, uuid_out);
					printf("Failed on node %s after %d reading nodes\n",
							uuid_out, *nodes);
					return rv;
				}
#ifdef PARANOID_MODE
				if(uuid_compare(bpnode_get_id(n), child->parent) != 0)
				{
					printf("Misalignment of parent-child in tree!\n");
					return BPTREE_OP_TREE_ERROR;
				}
#endif

				(*nodes)++;
				rv= bptree_index_scan_recursive(bps,child,
						prune_kv,failed_node,nodes,fp);

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
	fflush(fp);
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
	assert (bpnode_size(n) <= 2*BPTREE_MIN_DEGREE-1);
	uuid_unparse(bpnode_get_id(n), uuid_out_n);
	fprintf(fp, "\n\n");
	fprintf(fp, "\"N-%s\" [ label = < ", uuid_out_n);
	// use the first 8 chars of the uuid as the label
	strncpy(uuid_upper, uuid_out_n, 8);
	char ch = 'I';
	if(bpnode_is_leaf(n)) ch = 'L';
	uuid_upper[8] = '\0';
//	fprintf(fp, "%s (%d) [%d] %c :  ", uuid_upper, bpnode_size(n), n->tapioca_client_id, ch);
	if (bpnode_size(n) > 0) {
		bpnode_get_kv_ref(n, 0, &kv);
		bptree_key_value_to_string_kv(bps, &kv, s1);
		//	free(key_str);
		fprintf(fp, "  %s  ", s1);
		bpnode_get_kv_ref(n, bpnode_size(n)-1, &kv);
		bptree_key_value_to_string_kv(bps, &kv, s2);
		//	free(key_str);
		fprintf(fp, " %s ", s2);

	}
	fprintf(fp, " > ");
	if (uuid_is_null(bpnode_get_parent_id(n))) fprintf(fp, " , shape=\"diamond\"");
	else if (!bpnode_is_leaf(n)) fprintf(fp, " , shape=\"rect\"");
	fprintf(fp, " ] \n");
	if (!bpnode_is_leaf(n)) {
		for (c = 0; c <= bpnode_size(n); c++) {
			uuid_unparse(bpnode_get_child_id(n, c), uuid_out_c);
			fprintf(fp, "\"N-%s\" -> \"N-%s\"\n [color=\"#CC0000\"] ",
					uuid_out_n, uuid_out_c);
		}
		for (c = 0; c <= bpnode_size(n); c++) {
			bptree_node *next;
			next = read_node(bps,bpnode_get_child_id(n, c), &rv);
			if ( next == NULL) return -1;
			output_bptree_recursive(bps, next, level+1, fp);
		}
	} else {
		uuid_unparse(bpnode_get_next_id(n), uuid_out_c);
		if (!uuid_is_null(bpnode_get_next_id(n)))
			fprintf(fp, "\"N-%s\" -> \"N-%s\"\n ", uuid_out_n, uuid_out_c);
//		if (!uuid_is_null(n->prev_node))
//			uuid_unparse(n->prev_node, uuid_out_c);
//		fprintf(fp, "\"N-%s\" -> \"N-%s\" [weight = 100.0]\n ", uuid_out_n, uuid_out_c);
	}
//	// Print parent link
//	uuid_unparse(bpnode_get_parent_id(n), uuid_out_c);
//	if (!uuid_is_null(bpnode_get_parent_id(n)))
//		fprintf(fp, "\"N-%s\" -> \"N-%s\" [color=\"#CC0000\"] \n ",
//			uuid_out_n, uuid_out_c);
//
	return BPTREE_OP_SUCCESS;
}

void dump_node_info(bptree_session *bps, bptree_node *n)
{
	dump_node_info_fp(bps, n, stdout);
}

void dump_node_info_fp(bptree_session *bps, bptree_node *n, FILE *fp)
{
	int i, rv;
	char s1[512];
	char uuid_out[40];
	bptree_key_val kv;
	if (n == NULL) {
		fprintf (fp, "\tB+tree node is null!\n");
		return;
	}
	if ( (rv = is_node_sane(n)) != 0)
		fprintf(fp, "\tB+tree node failed sanity check rv %d!!\n", rv);

	fprintf(fp, "\tLeaf: %d\n", bpnode_is_leaf(n));

	uuid_unparse(bpnode_get_id(n), uuid_out);
	fprintf(fp, "\tSelf key: %s\n", uuid_out);
	uuid_unparse(bpnode_get_parent_id(n), uuid_out);
	fprintf(fp, "\tParent key: %s\n", uuid_out);
	uuid_unparse(bpnode_get_next_id(n), uuid_out);
	fprintf(fp, "\tNext key: %s\n", uuid_out);
	uuid_unparse(bpnode_get_prev_id(n), uuid_out);
	fprintf(fp, "\tPrev key: %s\n", uuid_out);


	fprintf(fp, "\tKey count: %d\n", bpnode_size(n));
	fprintf(fp, "\tKey/value sizes: ");
	for (i =0; i < bpnode_size(n); i++)
		fprintf (fp, "(%d, %d), ", 
			 bpnode_get_key_size(n,i),
			 bpnode_get_value_size(n,i));

	fprintf(fp, "\n\tKeys:\n\t\t");
	for (i =0; i < bpnode_size(n); i++)
	{
		bpnode_get_kv_ref(n, i, &kv);
		bptree_key_value_to_string_kv(bps, &kv, s1);
		fprintf(fp, " %s ", s1);
	}

	if(!bpnode_is_leaf(n))
	{
		fprintf(fp, "\n\tChild nodes:\n");
		for (i =0; i <= bpnode_size(n); i++)
		{
			uuid_unparse(bpnode_get_child_id(n, i), uuid_out);
			fprintf(fp, "\t\t%d: %s \n", i, uuid_out);
		}
	}

	fprintf(fp, "\n");

	fflush(fp);
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
	uuid_unparse(bpnode_get_id(n), uu);
	uu[8] = '\0'; // print first 8 bytes...
	for (i=0; i<bpnode_size(n); i++)
	{
		bptree_key_value_to_string(bps, n->keys[i], n->values[i],
				n->key_sizes[i], n->value_sizes[i], c);
		// Unsafe. Oh well
		strcpy(sptr, c);
		sptr += strlen(c);
		sprintf(sptr, ", ");
		sptr += 2;
	}
	if (bpnode_size(n) > 0)
	{
		sptr -= 2;
		bzero(sptr, 2);
	}
	sprintf(sptr, " Cnt: %d L: %d ID: %s WC: %d Ver: %d ST: %d ",
			bpnode_size(n), bpnode_is_leaf(n), uu, n->write_count, n->last_version,
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
				uuid_unparse(bpnode_get_id(n), uu);
				fprintf(trace_fp, "%d |\tBPT:%d\t%s\tCnt:%d\tLeaf:%d"
									"\tWriteCnt %d P/C Cl_id %d,%d\n",
						t->client_id, bpt_id, uu, bpnode_size(n), bpnode_is_leaf(n),
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

