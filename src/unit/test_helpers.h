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

int *init_new_int_array(int sz) ;
int sample_without_replacement(int *arr, int *n);



#endif
