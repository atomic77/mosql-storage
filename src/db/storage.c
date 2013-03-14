#include "storage.h"
#include "vset.h"
#include "hash.h"
#include "peer.h"

#include <stdlib.h>
#include <stdint.h>
#include <memory.h>
#include <assert.h>
#include <sys/queue.h>


#define STORAGE_TABLE_SIZE 12582917


TAILQ_HEAD(lru_head, key_entry_t);
LIST_HEAD(key_entry_head, key_entry_t);


#define ENTRY_IN_LRU(e)  (!(e.tqe_next == NULL && e.tqe_prev == NULL))


typedef struct __attribute__ ((packed)) key_entry_t {
    uint16_t size;
	vset values;
    LIST_ENTRY(key_entry_t) collisions;
    TAILQ_ENTRY(key_entry_t) lru;
    char key[0];
} key_entry;


static struct lru_head lru_list;
static long storage_key_entries;
static long storage_val_entries;
static long storage_current_size;
static long storage_gc_calls;
static struct key_entry_head storage_table[STORAGE_TABLE_SIZE];
static int gc_enabled = 1;

static consistent_hash node_id_for_hash;

static key_entry* key_entry_new(key* k);
static int key_entry_free(key_entry* kentry);
static int key_entry_local(key_entry* kentry);
static key_entry* find_key_entry(key* k);
static int key_cmp(key* k, key_entry* ke);
static unsigned int hash(char* k, int size);


int storage_init() {
    int i;
	
    storage_key_entries = 0;
	storage_val_entries = 0;
    storage_current_size = 0;
	storage_gc_calls = 0;
	
	node_id_for_hash = peer_get_default_hash();
	
    TAILQ_INIT(&lru_list);
    for (i = 0; i < STORAGE_TABLE_SIZE; i++)
        LIST_INIT(&storage_table[i]);

    return 1;
}


int storage_init2(consistent_hash chash) {
	storage_init();
	node_id_for_hash = chash;
	return 1;
}


void storage_free() {
    int i;
    key_entry* k;
    
    for (i = 0; i < STORAGE_TABLE_SIZE; i++) {
        while ((k = LIST_FIRST(&storage_table[i])) != NULL) {
            LIST_REMOVE(k, collisions);
            key_entry_free(k);
        }
    }
}


val* storage_get(key* k, int max_ver) {
	val* v;
	key_entry* kentry;
    if ((kentry = find_key_entry(k)) == NULL) {
		// printf("get %d %d %d %d\n", *(int*)k->data, -1, -1, max_ver);
        return NULL;
	}
	if ((v = vset_get(kentry->values, max_ver)) == NULL) {
		// printf("get %d %d %d %d\n", *(int*)k->data, -1, -1, max_ver);
		return NULL;	
	}
	// Move to lru head
	if (ENTRY_IN_LRU(kentry->lru)) {
		TAILQ_REMOVE(&lru_list, kentry, lru);
		TAILQ_INSERT_HEAD(&lru_list, kentry, lru);
	}
	// added this
	if (v->size == 0) {
		val_free(v);
		v = NULL;
	}
	
	// if (v != NULL)
		// printf("get %d %d %d %d\n", *(int*)k->data, *(int*)v->data, v->version, max_ver);
	// else
		// printf("get %d %d %d %d\n", *(int*)k->data, -1, -1, max_ver);
	
	return v;
}


int storage_put(key* k, val* v, int local, int force_cache) {
	unsigned int h;
    unsigned int i = -1;
    int kentry_is_new = 0;
    key_entry* kentry = NULL;

	// if (v->size > 0) {
		// if (force_cache)
	   		// printf("remote %d %d %d\n", *(int*)k->data, *(int*)v->data, v->version);
		// else
			// printf("put %d %d %d\n", *(int*)k->data, *(int*)v->data, v->version);
	// }

    //Find the Key entry, if not present create a new one
    if ((kentry = find_key_entry(k)) == NULL) {
		// If not local, drop it.
		if (!force_cache) {
			if (!local) {
				return 1;
			}
		}
		
        kentry = key_entry_new(k);
		h = hash((char*)k->data, k->size);
        i = h % STORAGE_TABLE_SIZE;
        LIST_INSERT_HEAD(&storage_table[i], kentry, collisions);
        kentry_is_new = 1;
    }
    

    // If key became local
    if (local && ENTRY_IN_LRU(kentry->lru)) {
        // kentry was already there, remove it from LRU
        // if (!kentry_is_new) {
			TAILQ_REMOVE(&lru_list, kentry, lru);
			memset(&kentry->lru, 0, sizeof(kentry->lru));
			assert(!ENTRY_IN_LRU(kentry->lru));
        // }
    }

	storage_val_entries -= vset_count(kentry->values);
	storage_current_size -= vset_allocated_bytes(kentry->values);
	// Add to the set of values
	vset_add(kentry->values, v);
	storage_val_entries += vset_count(kentry->values);
	storage_current_size += vset_allocated_bytes(kentry->values);
	
	// Insert in LRU if item is cached and if it is not already there
	if (!local && !ENTRY_IN_LRU(kentry->lru)) {
        TAILQ_INSERT_HEAD(&lru_list, kentry, lru);
	}
	
    return 1;
}


long storage_get_current_size() {
    return storage_current_size;
}


long storage_key_count() {
    return storage_key_entries;
}


long storage_val_count() {
    return storage_val_entries;
}


long storage_gc_count() {
	return storage_gc_calls;
}


void storage_gc_at_least(int bytes) {
	key_entry* kentry;
    
	if (!gc_enabled)
		return;
	
	storage_gc_calls++;
	// printf("garbage\n");
	while ((bytes > 0)) {
		kentry = TAILQ_LAST(&lru_list, lru_head);
		if (kentry == NULL)
			return;

		TAILQ_REMOVE(&lru_list, kentry, lru);
		memset(&kentry->lru, 0, sizeof(kentry->lru));
		if (!key_entry_local(kentry)) {
			LIST_REMOVE(kentry, collisions);
			bytes -= key_entry_free(kentry);
		}
	}
}


int storage_iterate(int version, void (iter)(key*, val*, void*), void* arg) {
	int i;
	key_entry* kentry;
	key k;
	val* v;
	int count = 0;
	
	for (i = 0; i < STORAGE_TABLE_SIZE; i++) {
	    LIST_FOREACH(kentry, &storage_table[i], collisions) {
			if (key_entry_local(kentry)) {
				k.size = kentry->size;
				k.data = kentry->key;
				v = vset_get(kentry->values, version);
				iter(&k, v, arg);
				val_free(v);
				count++;
			}
		}
	}
	return count;
}


void storage_gc_start() {
	gc_enabled = 1;
	// printf("gc start\n");
}


void storage_gc_stop() {
	gc_enabled = 0;
	// printf("gc stop\n");
}


static key_entry* key_entry_new(key* k) {
	int size;
    key_entry* kentry;
    
	size = sizeof(key_entry) + k->size;
    kentry = DB_MALLOC(size);
	memset(kentry, 0, size);
    kentry->size = k->size;
    memcpy(kentry->key, k->data, k->size);
	kentry->values = vset_new();
    storage_key_entries++;
    storage_current_size += (size + vset_allocated_bytes(kentry->values));
    
    return kentry;
}


static int key_entry_free(key_entry* kentry) {
    int bytes;
    
    bytes = sizeof(key_entry) + kentry->size + vset_allocated_bytes(kentry->values);
    
    storage_key_entries--;
	storage_val_entries -= vset_count(kentry->values);
    storage_current_size -= bytes;

	vset_free(kentry->values);
    DB_FREE(kentry);
    
    return bytes;
}


static int key_entry_local(key_entry* kentry) {
	unsigned int h;
	h = joat_hash(kentry->key, kentry->size);
	return node_id_for_hash(h) == NodeID;
}


static key_entry* find_key_entry(key* k) {
    unsigned int h, i;
    key_entry* kentry;
    
    h = hash((char*) k->data, k->size);
    i = h % STORAGE_TABLE_SIZE;
    
    LIST_FOREACH(kentry, &storage_table[i], collisions) {
        if (key_cmp(k, kentry)) {
            return kentry;
        }
    }
    return NULL;
}


static int key_cmp(key* k, key_entry* ke) {
    if (k->size != ke->size)
        return 0;
    
    if (memcmp(k->data, ke->key, k->size) == 0)
        return 1;
    else
        return 0;
}


static unsigned int hash(char* k, int size) {
	if (size == sizeof(int)) {
		unsigned int* h;
		h = (unsigned int*)k;
		return *h;
	}
	return joat_hash(k, size);
}
