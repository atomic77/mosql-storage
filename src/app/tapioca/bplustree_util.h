
typedef struct bptree_key_val {
	unsigned char *k;
	unsigned char *v;
	int32_t ksize; // these probably could be 16-bit but too much code to change
	int32_t vsize;
} bptree_key_val;

void free_key_val(bptree_key_val **kv);
