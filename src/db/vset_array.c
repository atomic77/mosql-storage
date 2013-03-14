#include "vset.h"

#ifdef USE_VSET_ARRAY

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
	val_entry** versions;
};


static val_entry* val_entry_new(vset s, int size);
static void val_entry_free(vset s, val_entry* v);
static int find_index(vset s, int version);


vset vset_new() {
	int i;
	vset s;
	s = malloc(sizeof(struct vset_t));
	if (s == NULL) return NULL;
	s->versions = malloc(sizeof(val_entry*) * StorageMaxOldVersions);
	if (s->versions == NULL) {free(s); return NULL;}
	for (i = 0; i < StorageMaxOldVersions; i++)
		s->versions[i] = NULL;
	s->size = sizeof(struct vset_t) + (sizeof(val_entry*) * StorageMaxOldVersions);
	s->count = 0;
	return s;
}


void vset_free(vset s) {
	int i;
	for (i = 0; i < StorageMaxOldVersions; i++)
		free(s->versions[i]);
	free(s->versions);
	free(s);
}


int vset_add(vset s, val* v) {
	int i;
	i = find_index(s, v->version);
	if (s->versions[i] == NULL) {
		s->versions[i] = val_entry_new(s, v->size);
		if (s->versions[i] == NULL) return -1;
	} else if (s->versions[i]->size != v->size) {
		val_entry_free(s, s->versions[i]);
		s->versions[i] = val_entry_new(s, v->size);
		if (s->versions[i] == NULL) {s->versions[i] = NULL; return -1;}
	}
	s->versions[i]->version = v->version;
	memcpy(s->versions[i]->data, v->data, v->size);
	return 0;
}


val* vset_get(vset s, int version) {
	val_entry* v = NULL;
	int i, smallest = -1, smallest_index = -1;	
	for (i = 0; i < StorageMaxOldVersions; i++) {
		v = s->versions[i];
		if (v == NULL) continue;
		if ((v->version <= version) && (v->version > smallest)) {
			smallest = v->version;
			smallest_index = i;
		}
	}
	if (smallest_index == -1)
		return NULL;
	v = s->versions[smallest_index];
	return versioned_val_new(v->data, v->size, v->version);
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
	s->count++;
	return v;
}


static void val_entry_free(vset s, val_entry* v) {
	s->size -= (sizeof(val_entry) + v->size);
	s->count--;
	free(v);
}

// returns either:
//  - the index that contains the same version
//	- the first index set to NULL
//  - the index that contains the oldest entry
static int find_index(vset s, int version) {
	val_entry* v;
	int i, oldest = INT_MAX, oldest_index = 0;
	for (i = 0; i < StorageMaxOldVersions; i++) {
		v = s->versions[i];
		if (v == NULL) {
			oldest_index = i;
			oldest = 0;
			continue;
		}
		if (v->version == version)
			return i;
		if (v->version < oldest) {
			oldest = v->version;
			oldest_index = i;
		}
	}
	return oldest_index;
}

#endif
