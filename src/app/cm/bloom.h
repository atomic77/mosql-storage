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

#ifndef _BLOOM_H_
#define _BLOOM_H_

#include "hash.h"

typedef struct bloom_t {
    int n_hash;
    hash_fun* hash_funcs;
    unsigned int bit_size;
    unsigned char bitmap[0];
} bloom;


bloom* bloom_new(int bits, int n_hash);

void bloom_destroy(bloom* b);

void bloom_add(bloom* b, char* data, int size);

void bloom_add_hashes(bloom* b, unsigned int* h);

int bloom_contains(bloom* b, char* data, int size);

unsigned int* bloom_gen_hashes(bloom* b, char* data, int size);

int bloom_contains_hashes(bloom* b, unsigned int* h);

void bloom_clear(bloom* b);

void bloom_print(bloom* b);

void bloom_set_bit(bloom* b, unsigned int h);

void bloom_unset_bit(bloom* b, unsigned int h);

int bloom_bit_is_set(bloom* b, unsigned int h);

void bloom_union(bloom* b1, bloom* b2);

int bloom_intersect(bloom* b1, bloom* b2);

#endif /* _BLOOM_H_ */
