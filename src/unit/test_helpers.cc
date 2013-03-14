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
