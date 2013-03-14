#include <stdlib.h>
#include <memory.h>

#include "dsmDB_priv.h"


key* key_new (void* data, int size) {
    key* k;
    
    k = DB_MALLOC (sizeof (key));
    k->data = DB_MALLOC (size);
    memcpy (k->data, data, size);
    k->size = size;
    
    return k;
}

val* versioned_val_new (void* data, int size, int version) {
    val* v;
    
    v = DB_MALLOC (sizeof (val));
    if (size > 0) {
        v->data = DB_MALLOC (size);
        memcpy (v->data, data, size);
    }
    v->size = size;
    v->version = version;
    
    return v;

}

val* val_new (void* data, int size) {
    
    return versioned_val_new(data, size, -1);
    
}


void val_free (val* v) {
    if (v->size > 0)
        DB_FREE (v->data);
    DB_FREE (v);
}


void key_free (key* k) {
    DB_FREE (k->data);
    DB_FREE (k);
}

