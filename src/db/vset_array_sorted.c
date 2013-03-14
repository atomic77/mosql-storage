#include "vset.h"

#ifdef USE_VSET_ARRAY_SORTED

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>


typedef struct val_entry_t {
    int version;
    uint16_t size;
    char data[0];
} val_entry;


struct vset_t {
	uint16_t size;
	uint16_t count;
	val_entry* versions[0];
};


static val_entry* val_entry_new(vset s, int size);
static void val_entry_free(vset s, val_entry* v);


vset vset_new() {
	vset s;
	int size;
	size = (sizeof(struct vset_t) + (sizeof(val_entry*) * StorageMaxOldVersions));
	s = malloc(size);
	s->size = size;
	s->count = 0;
	return s;
}


void vset_free(vset s) {
	int i;
	for (i = 0; i < s->count; i++)
		free(s->versions[i]);
	free(s);
}


int vset_add(vset s, val* v) {
	int i = 0;
	val_entry* ventry = NULL;
	
	// Delete old value
	if (s->count >= StorageMaxOldVersions) {
		s->count--;
		ventry = s->versions[s->count];
		if (ventry->size != v->size) {
			val_entry_free(s, ventry);
			ventry = val_entry_new(s, v->size);
		}
		s->versions[s->count] = NULL;
	}
	
	if (ventry == NULL)
		ventry = val_entry_new(s, v->size);
	
	ventry->version = v->version;
	memcpy(ventry->data, v->data, v->size);
	
	while (i < s->count) {
		if (ventry->version > s->versions[i]->version) {
			memmove(&(s->versions[i+1]), &(s->versions[i]), (s->count - i) * sizeof(val_entry*));
			s->versions[i] = ventry;
			s->count++;
			return 0;
		}
		if (ventry->version == s->versions[i]->version) {
			val_entry_free(s, s->versions[i]);
			s->versions[i] = ventry;		
			return 0;
		}
		i++;
	}
	
	s->versions[s->count] = ventry;
	s->count++;
	return 0;
}


val* vset_get(vset s, int v) {
	int i;
	val_entry* ventry = NULL;
	for (i = 0; i < s->count; i++) {
		ventry = s->versions[i];
		if (ventry->version <= v)
			return versioned_val_new(ventry->data, ventry->size, ventry->version);
	}
	return NULL;
}


int vset_allocated_bytes(vset s) {
	return s->size;
}


int vset_count(vset s) {
	return s->count;
}


static val_entry* val_entry_new(vset s, int size) {
	val_entry* v;
	v = malloc(sizeof(val_entry) + size);
	if (v == NULL) return NULL;
	v->size = size;
	s->size += size;
	return v;
}


static void val_entry_free(vset s, val_entry* v) {
	s->size -= (sizeof(val_entry) + v->size);
	free(v);
}

#endif
