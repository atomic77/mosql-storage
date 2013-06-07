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

#include "test_helpers.h"
#include "storage.h"
#include <cstring>


key* createKey(int i) {
	return key_new(&i, sizeof(int));
}


key* createKey(const std::string &s) {
	return key_new((char*)s.c_str(), s.size());
}


val* createVal(int i, int version) {
	return versioned_val_new(&i, sizeof(int), version);
}


val* createVal(const std::string &s, int version) {
	return versioned_val_new((char*)s.c_str(), s.size(), version);
}


bool valEqual(val* v1, val* v2) {
	if (v1->size != v2->size) {
		// TODO use gtest's error reporting here...
		printf("valEqual: v1 has size %d while v2 has size %d\n",
			v1->size, v2->size);
		return false;
	}
	
	if (memcmp(v1->data, v2->data, v1->size) == 0)
        return true;
    return false;
}


bool valVersionEqual(val* v1, val* v2) {
	return valEqual(v1, v2) && (v1->version == v2->version);
}


bool matchStorageVersion(key* k, val* v, int version) {
	int equal;
	val* rv;
	rv = storage_get(k, version);
	if (rv == NULL && v == NULL)
		return true;
	if (rv == NULL || v == NULL)
		return false;
	equal = valVersionEqual(v, rv);
	val_free(rv);
	return equal;
}
