#include "vset.h"

#ifdef USE_VSET_LIST

#include <stdlib.h>
#include <sys/queue.h>
#include <stdint.h>
#include <string.h>


TAILQ_HEAD(values_head, val_entry_t);


typedef struct val_entry_t {
    uint16_t size;
    int version;
    TAILQ_ENTRY(val_entry_t) values;
    char val[0];
} val_entry;


struct vset_t {
	int8_t count;
	int alloc_bytes;
	struct values_head values_list;
};


static val_entry* val_entry_new(vset s, val* v);
static int val_entry_free(vset s, val_entry* ventry);


vset vset_new() {
	vset v;
	v = malloc(sizeof(struct vset_t));
	if (v == NULL) return NULL;
	v->count = 0;
	v->alloc_bytes = sizeof(struct vset_t);
	TAILQ_INIT(&v->values_list);
	return v;
}


void vset_free(vset s) {
	val_entry* v;
	while ((v = TAILQ_FIRST(&s->values_list)) != NULL) {
        TAILQ_REMOVE(&s->values_list, v, values);
        val_entry_free(s, v);
    }
	free(s);
}


int vset_add(vset s, val* v) {
	val_entry* itr;
	val_entry* ventry = NULL;
	
	if (s->count >= StorageMaxOldVersions) {
    	itr = TAILQ_LAST(&s->values_list, values_head);
		if (itr->size == v->size) {
			ventry = itr;
			ventry->version = v->version;
		    memcpy(ventry->val, v->data, v->size);
		} else {
	    	val_entry_free(s, itr);
		}
        s->count--;
        TAILQ_REMOVE(&s->values_list, itr, values);
	}

	if (ventry == NULL)
		ventry = val_entry_new(s, v);
	
	//Search trough value versions list
    itr = TAILQ_FIRST(&s->values_list);

    //Iterate to find the right place, values_list is sorted by version
    while (itr != NULL) {       
        //Insert before itr
        if (ventry->version > itr->version) {
            s->count++;
            TAILQ_INSERT_BEFORE(itr, ventry, values);
			break;
			
        //Found same version, overwrite
        } else if (ventry->version == itr->version) {
            TAILQ_INSERT_BEFORE(itr, ventry, values);
            TAILQ_REMOVE(&s->values_list, itr, values);
            val_entry_free(s, itr);
            break;
        
        //Keep iterating
        } else {
            itr = TAILQ_NEXT(itr, values);
        }
    }
    
    // Either no value at all, or item needs to be inserted in last position
    if (itr == NULL) {
        TAILQ_INSERT_TAIL(&s->values_list, ventry, values);
        s->count++;
    }
	
	return 0;
}


val* vset_get(vset s, int v) {
    val_entry* ventry;

    TAILQ_FOREACH(ventry, &s->values_list, values) {
        if (ventry->version <= v)
            return versioned_val_new(ventry->val, 
									 ventry->size,
									 ventry->version);
    }
    return NULL;
}


int vset_allocated_bytes(vset s) {
	return s->alloc_bytes;
}


int vset_count(vset s) {
	return s->count;
}


static val_entry* val_entry_new(vset s, val* v) {
    val_entry* ventry;
    ventry = malloc(sizeof(val_entry) + v->size);
	if (ventry == NULL) return NULL;
    ventry->size = v->size;
    ventry->version = v->version;
    memcpy(ventry->val, v->data, v->size);
 	s->alloc_bytes += (sizeof(val_entry) + v->size);
    return ventry;
}


static int val_entry_free(vset s, val_entry* ventry) {
    int bytes;
    bytes = sizeof(val_entry) + ventry->size;
    free(ventry);
	s->alloc_bytes -= bytes;
    return bytes;
}

#endif
