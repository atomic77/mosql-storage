#include "vset.h"

#ifdef USE_VSET_ARRAY_CACHE

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>


typedef struct __attribute__ ((packed)) val_entry_t {
    int version;
    uint16_t size;
    void* data;
} val_entry;


struct __attribute__ ((packed)) vset_t  {
	uint16_t count;
	val_entry versions[0];
};


vset vset_new() {
	vset s;
	int size;
	size = (sizeof(struct vset_t) + (sizeof(val_entry) * StorageMaxOldVersions));
	s = malloc(size);
	s->count = 0;
	return s;
}


void vset_free(vset s) {
	int i;
	for (i = 0; i < s->count; i++)
		free(s->versions[i].data);
	free(s);
}


int vset_add(vset s, val* v) {
	int i = 0;
	val_entry ventry;
	
	ventry.size = v->size;
	ventry.version = v->version;
	ventry.data = NULL;
	
	// Delete old value
	if (s->count >= StorageMaxOldVersions) {
		s->count--;
		if (s->versions[s->count].size != v->size) {
			free(s->versions[s->count].data);
		} else {
			ventry.data = s->versions[s->count].data;
		}
		s->versions[s->count].data = NULL;
	}
	
	if (ventry.data == NULL) {
		ventry.data = malloc(v->size);
	}
	memcpy(ventry.data, v->data, v->size);
	
	while (i < s->count) {
		if (ventry.version > s->versions[i].version) {
			memmove(&(s->versions[i+1]), &(s->versions[i]), (s->count - i)*sizeof(val_entry));
			s->versions[i] = ventry;
			s->count++;
			return 0;
		}
		if (ventry.version == s->versions[i].version) {
			free(s->versions[i].data);
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
	for (i = 0; i < s->count; i++) {
		if (s->versions[i].version <= v)
			return versioned_val_new(s->versions[i].data, 
									 s->versions[i].size,
									 s->versions[i].version);
	}
	return NULL;
}


int vset_allocated_bytes(vset s) {
	int i;
	int bytes = (sizeof(struct vset_t) + (sizeof(val_entry) * StorageMaxOldVersions));
	for (i = 0; i < s->count; i++) {
		bytes += s->versions[i].size;
	}
	return bytes;
}


int vset_count(vset s) {
	return s->count;
}

#endif
