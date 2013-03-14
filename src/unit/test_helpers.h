#ifndef _TEST_HELPERS_H_
#define _TEST_HELPERS_H_

#include "dsmDB_priv.h"
#include <string>

key* createKey(int i);
key* createKey(const std::string &s);
val* createVal(int i, int version);
val* createVal(const std::string &s, int version);
bool valEqual(val* v1, val* v2);
bool valVersionEqual(val* v1, val* v2);
bool matchStorageVersion(key* k, val* v, int version);

#endif
