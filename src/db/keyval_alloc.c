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

