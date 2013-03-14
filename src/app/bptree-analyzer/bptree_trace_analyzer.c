
#include "bplustree_util.h"
#include "hash.h"
#include "hashtable_itr.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <getopt.h>

struct option long_options[] =
{
		/* These options set a flag. */
		{"trace-file-suffix", 		required_argument, 0, 'f'},
		{"trace-file-count",   	required_argument, 0, 'c'},
		{"help",  			no_argument,       0, 'h'},
		{0, 0, 0, 0}
};

//const char *short_opts = "y:t:n:c:s:p:q:k:i:l:drh";
const char *short_opts = "f:c:h";

enum bench_test_type
{
	BENCH_INSERT_SEQUENTIAL=0,
	BENCH_INSERT_RANDOM_PK_IN_RANGE=1,
	BENCH_SELECT_RANDOM_PK_IN_RANGE=2,
	BENCH_UPDATE_RANDOM_PK_IN_RANGE=3,
	BENCH_INSERT_RANDOM_MULTIPART_PK_IN_RANGE=4,
	BENCH_SELECT_RANDOM_MULTIPART_PK_IN_RANGE=5,
	BENCH_UPDATE_RANDOM_MULTIPART_PK_IN_RANGE=6
};

#define TAPIOCA_ROW_PACKET_HEADER 0x3
#define BPTREE_META_NODE_PACKET_HEADER 0x4
#define BPTREE_NODE_PACKET_HEADER 0x5

static int trace_file_count = -1;
static char* trace_file_suffix;

typedef struct stats {
	int num_keys, tot_key_sz, tot_val_sz, commits , failed ;
	int dump_node_ids ;
	int row_keys, row_val_sz, btree_keys , btree_val_sz , meta_keys;
	struct timespec tm_st, tm_end;
} st_stats ;

static unsigned int hash_from_key(void* k) {
	return joat_hash(k, sizeof(int));
}

typedef struct bpt_node_id {
	uint16_t bpt_id;
	uuid_t node_key;
} bpt_node_id;

typedef struct bpt_node_id_data {
	uuid_t children[BPTREE_NODE_SIZE+1];
	int references;
	int key_count;
	int leaf;
	uuid_t parent;
	int parent_child_valid;
} bpt_node_id_data;

static int bpt_node_id_cmp(void* k1, void* k2) {
	bpt_node_id* a = (bpt_node_id *)k1;
	bpt_node_id* b = (bpt_node_id *)k2;
	return (a->bpt_id == b->bpt_id &&
			uuid_compare(a->node_key, b->node_key) == 0);
}

struct hashtable *bpt_node_ids;
struct hashtable *bpt_meta_ids;

void output_final_stats(st_stats *t);
void update_stats(unsigned char *k, int ksize, int vsize, st_stats *t);

void print_usage()
{

	struct option opt = long_options[0];
	int i = 0;
	fprintf(stderr, "Command line options:\n");
	while (1)
	{
		if (opt.name == 0)
			break;
		fprintf(stderr, "\t--%s , -%c \n", opt.name, opt.val);
		i++;
		opt = long_options[i];
	}
	fprintf(stderr,
			"See enum bench_test_type in header file for test numbers\n");
}

void pretty_print_node_data(bpt_node_id *k, bpt_node_id_data *v, FILE *fp)
{
	int i;
	char uuid_out[40];
	char s[256];
	bptree_key_val kv;
	uuid_unparse(k->node_key, uuid_out);
	fprintf(fp, "Bpt ID %d\nNodeID: %s\nKeyCount: %d\nLeaf %d\nReferences:%d \n",
			k->bpt_id, uuid_out, v->key_count, v->leaf, v->references);
	uuid_unparse(v->parent, uuid_out);
	fprintf(fp, "Parent node: %s\n", uuid_out);
	fprintf(fp, "Parent-Child valid: %d\n", v->parent_child_valid);

	if (!v->leaf) {
		fprintf(fp, "Children:\n");
		for(i = 0; i <= v->key_count; i++)
		{
			uuid_unparse(v->children[i], uuid_out);
			fprintf(fp, "\t%d: %s\n" , i, uuid_out);
		}
	}
	fprintf(fp, "\n");
}

void dump_meta_node_contents(void)
{
	struct hashtable_itr *itr;
	bpt_node_id *k;
	bpt_node_id_data *v;
	char uuid_out[40];
	if(hashtable_count(bpt_meta_ids) <= 0)
	{
		fprintf(stderr, "No meta keys found!\n");
		return;
	}
	itr = (struct hashtable_itr *)hashtable_iterator(bpt_meta_ids);
	do {
		k = (bpt_node_id *) hashtable_iterator_key(itr);
		v = (bpt_node_id_data *) hashtable_iterator_value(itr);

		uuid_unparse(k->node_key, uuid_out);
		printf("BptID %d NodeID: %s\n", k->bpt_id, uuid_out);
		v = hashtable_search(bpt_node_ids, k);
		traverse_bpt_tree(k, v);

	} while (hashtable_iterator_advance(itr));

	free(itr);

}

void dump_node_contents(void)
{
	struct hashtable_itr *itr;
	bpt_node_id *k;
	bpt_node_id_data *v;
	if(hashtable_count(bpt_node_ids) <= 0)
	{
		fprintf(stderr, "No bptree keys found!\n");
		return;
	}
	itr = (struct hashtable_itr *)hashtable_iterator(bpt_node_ids);
	do {
		k = (bpt_node_id *) hashtable_iterator_key(itr);
		v = (bpt_node_id_data *) hashtable_iterator_value(itr);
//		if(v->references != 1)
			pretty_print_node_data(k,v,stdout);

	} while (hashtable_iterator_advance(itr));

	free(itr);

}

int traverse_bpt_tree(bpt_node_id *root)
{
	int i;
	bpt_node_id c_k;
	bpt_node_id_data *bn_v, *c_v;

	bn_v = hashtable_search(bpt_node_ids, root);
	bn_v->references++;
	if(!bn_v->leaf)
	{
		for(i = 0; i <= bn_v->key_count; i++)
		{
			c_k.bpt_id = root->bpt_id;
			uuid_copy(c_k.node_key, bn_v->children[i]);
			c_v = hashtable_search(bpt_node_ids, &c_k);
			c_v->parent_child_valid =
					(uuid_compare(root->node_key, c_v->parent) == 0);
			traverse_bpt_tree(&c_k);
		}
	}
	else
	{
		return 1;
	}

}

void output_final_stats(st_stats *t) {
	fprintf(stderr, "\n");
	fprintf(stderr, "Total keys: %d\n", t->num_keys);
	fprintf(stderr, "Key size: %d\n", t->tot_key_sz);
	fprintf(stderr, "Val size: %d\n", t->tot_val_sz);
	fprintf(stderr, "Row keys: %d, total size %d, avg row_sz: %.1f  \n",
			t->row_keys, t->row_val_sz, (float)(t->row_val_sz/t->row_keys));
	fprintf(stderr, "B+Tree keys: %d, total size %d, avg node sz: %.1f \n",
			t->btree_keys, t->btree_val_sz, (float)(t->btree_val_sz / t->btree_keys));
	fprintf(stderr, "Meta keys: %d \n", t->meta_keys);
	fprintf(stderr, "Total commits: %d\n", t->commits);
	fprintf(stderr, "Failed commits: %d\n\n", t->failed);

	fprintf(stderr, "Final hashtable sizes: %d %d \n",
			hashtable_count(bpt_meta_ids),
			hashtable_count(bpt_node_ids));

	dump_meta_node_contents();
	dump_node_contents();

}

void update_stats(unsigned char *k, int ksize, int vsize, st_stats *t) {
	if(*k ==  TAPIOCA_ROW_PACKET_HEADER) {
		t->row_keys++;
		t->row_val_sz+= vsize;
	} else if(*k == BPTREE_NODE_PACKET_HEADER) {
		t->btree_keys++;
		t->btree_val_sz += vsize;
	} else if (*k == BPTREE_META_NODE_PACKET_HEADER) {
		t->meta_keys++;
	} else {
//		fprintf(stderr, "Other key header found at len %d key %s\n", ksize, k);
	}
	t->num_keys ++;
	t->tot_key_sz+= ksize;
	t->tot_val_sz+= vsize;

	if ((t->num_keys) %  (50*1000) == 0) {
		clock_gettime(CLOCK_MONOTONIC, &t->tm_end);
		float secs = (t->tm_end.tv_sec-t->tm_st.tv_sec) +
				(t->tm_end.tv_nsec - t->tm_st.tv_nsec) / 1000000000.0;
		float drate = (t->tot_key_sz+t->tot_val_sz) / secs;
		float krate = t->num_keys / secs;
		drate = drate / (1024*1024);
		fprintf(stderr, "%d keys, %.1f MB k_sz, %.1f MB v_sz, lpos %d"
				" time %.1f Rate %.1f MB/s %.1f k/s\n",
				t->num_keys, (t->tot_key_sz/1024/1024.0), (t->tot_val_sz/1024/1024.0),
				secs,drate, krate);
	}

}

void parse_opts(int argc, char **argv)
{
	int opt_idx;
	char ch;

	while ((ch = getopt_long(argc, argv, short_opts, long_options, &opt_idx))
			!= -1)
	{
		switch (ch)
		{
		case 'f':
			asprintf(&trace_file_suffix, "%s", optarg);
			break;
			// Example code to use as more options are added to this tool
		case 'c':
			trace_file_count = atoi(optarg);
			break;
		default:
		case 'h':
			print_usage();
			exit(1);
		}
	}

	if (argc < 2 || trace_file_count == -1)
	{
		print_usage();
		exit(1);
	}
}

void process_bptree_node(unsigned char *k, unsigned char *v, int ksize,
		int vsize, st_stats *t)
{
	int i;
	size_t nsize;
	bptree_node *n;
	bpt_node_id *bn_k = malloc(sizeof(bpt_node_id));
	bpt_node_id_data *bn_v;

	t->btree_keys++;
	t->btree_val_sz += vsize;
	memcpy(&bn_k->bpt_id, k+1, sizeof(int16_t));
	n = unmarshall_bptree_node(v, vsize, &nsize);
	if (n == NULL)
	{
//		printf("BTree node corrupt\n");
		return;
	}

	uuid_copy(bn_k->node_key, n->self_key);
	bn_v = hashtable_search(bpt_node_ids, bn_k);
	// Here we should compare if the children are the same!
	// Seems highly unlikely though
	if (bn_v == NULL)
	{
		bn_v = malloc(sizeof(bpt_node_id_data));
		bn_v->key_count = n->key_count;
		bn_v->leaf = n->leaf;
		uuid_copy(bn_v->parent, n->parent);
		for(i = 0; i<=BPTREE_NODE_SIZE; i++)
		{
			uuid_copy(bn_v->children[i], n->children[i]);
		}
		bn_v->references = 0;
		bn_v->parent_child_valid = -1;
		hashtable_insert(bpt_node_ids, bn_k, bn_v);
	}
	else
	{
		bn_v->references++;
	}
}

void process_bptree_meta_node(unsigned char *k, unsigned char *v, int ksize,
		int vsize, st_stats *t)
{
	int i;
	size_t nsize;
	bptree_meta_node *m;
	bpt_node_id *bn_k = malloc(sizeof(bpt_node_id));
	bpt_node_id_data *bn_v;

	t->meta_keys++;
	memcpy(&bn_k->bpt_id, k+1, sizeof(int16_t));
	m = unmarshall_bptree_meta_node(v, vsize);
	if (m == NULL)
	{
//		printf("BTree node corrupt\n");
		return;
	}

	uuid_copy(bn_k->node_key, m->root_key);
	bn_v = hashtable_search(bpt_meta_ids, bn_k);
	if (bn_v == NULL)
	{
		bn_v = malloc(sizeof(bpt_node_id_data));
		for(i = 0; i<=BPTREE_NODE_SIZE; i++)
		{
			uuid_clear(bn_v->children[i]);
		}
		bn_v->references=1;
		hashtable_insert(bpt_meta_ids, bn_k, bn_v);
	}
	else
	{
		bn_v->references++;
	}
}

void process_file(int file_num, st_stats *t)
{
	int ksize, vsize;
//	unsigned char k[65000], v[65000];
	unsigned char *k, *v;
	k = malloc(65000);
	v = malloc(65000);
	FILE *fp;
	char *filename;

	asprintf(&filename, "%s-%d.bin", trace_file_suffix, file_num);
	fp = fopen(filename,"r");
	if (fp == NULL) {
		printf("Failed to open %s\n",filename);
		exit (-1);
	}
	printf("Opened file %s , fp %p \n", filename, fp);
	clock_gettime(CLOCK_MONOTONIC, &t->tm_st);
	while (fread(&ksize,sizeof(int),1,fp) != 0) {
		fread(k,ksize,1,fp);
		fread(&vsize,sizeof(int),1,fp);
		fread(v,vsize,1,fp);
		switch (*k) {
			case TAPIOCA_ROW_PACKET_HEADER:
				t->row_keys++;
				t->row_val_sz+= vsize;
				break;
			case BPTREE_NODE_PACKET_HEADER:
				process_bptree_node(k, v, ksize, vsize, t);
				break;
			case BPTREE_META_NODE_PACKET_HEADER:
				process_bptree_meta_node(k, v, ksize, vsize, t);
				break;
			default:
				break;
		}
		update_stats(k,ksize,vsize,t);
	}
	free(k);
	free(v);
/*	err:
		fclose(fp);
		fprintf(stderr, "Error while loading (%d records loaded)\n", t.num_keys);
	        fprintf(stderr, "Current k/v sizes: %d %d rv, rv2: %d %d \n",
                		ksize, vsize, rv, rv2);
		return -1;*/
}

int main(int argc, char **argv) {
	int rv,rv2=123,ksize,vsize, i, comm_sz = 0, dirty = 0;
	st_stats t;
	bzero(&t, sizeof(st_stats));
	parse_opts(argc, argv);

	bpt_node_ids =
			create_hashtable(64, hash_from_key, bpt_node_id_cmp, free);
	bpt_meta_ids =
			create_hashtable(64, hash_from_key, bpt_node_id_cmp, free);

	if (bpt_node_ids == NULL) return -1;
	if (bpt_meta_ids == NULL) return -1;

	for(i = 0; i< trace_file_count; i++)
	{
		process_file(i,&t);
	}

	output_final_stats(&t);
	exit(0);
}

