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
#include <assert.h>

#include "bloom.h"
#include "dsmDB_priv.h"

#define SET_BIT(char, index) (char |= (1 << index))
#define UNSET_BIT(char, index) (char &= ~(1 << index))
#define GET_BIT(char, index) (char &  (1 << index))

#define MOD8(n) (n & 7)
#define MOD4(n) (n & 3)
#define DIV8(n) (n >> 3)


#define HASHES_SIZE 2
static hash_fun hashes[HASHES_SIZE] = {joat_hash, djb2_hash};

static void __bloom_set_bit(bloom* b, unsigned int h);
static void __bloom_unset_bit(bloom* b, unsigned int h);
static int __bloom_bit_is_set(bloom* b, unsigned int h);


bloom* bloom_new(int bits, int n_hash) {
    bloom* b;
    int bytes;
    
	if (n_hash > HASHES_SIZE)
        return NULL;
    
    if (MOD8(bits) != 0)
        bits += 8 - (MOD8(bits));
        
    bytes = DIV8(bits);
    
    // round to multiple of 4 bytes
    if (MOD4(bytes) != 0)
        bytes += 4 - MOD4(bytes);
        
    bits = bytes * 8;
    
    b = DB_MALLOC(sizeof(bloom) + bytes);
    b->bit_size = bits;
    b->n_hash = n_hash;
    b->hash_funcs = hashes;

    memset (b->bitmap, 0, bytes);

    return b;
}


void bloom_destroy(bloom* b) {
    DB_FREE(b);
}


void bloom_set_bit(bloom* b, unsigned int h) {
    __bloom_set_bit(b, h);
}


void bloom_unset_bit(bloom* b, unsigned int h) {
    __bloom_unset_bit(b, h);
}


int bloom_bit_is_set(bloom* b, unsigned int h) {
    return __bloom_bit_is_set(b, h);
}


void bloom_add(bloom* b, char* data, int size) {
    int i;
    unsigned int h;
    for (i = 0; i < b->n_hash; i++) {
        h = b->hash_funcs[i](data, size);
        __bloom_set_bit(b, h);
    }
}


void bloom_add_hashes(bloom* b, unsigned int* h) {
    int i;
    for (i = 0; i < b->n_hash; i++)
        __bloom_set_bit(b, h[i]);
}


int bloom_contains(bloom* b, char* data, int size) {
    int i;
    unsigned int h;
    for (i = 0; i < b->n_hash; i++) {
        h = b->hash_funcs[i](data, size);
        if (!__bloom_bit_is_set(b, h))
            return 0;
    }
    return 1;
}


unsigned int* bloom_gen_hashes(bloom* b, char* data, int size) {
    int i;
    unsigned int* a = DB_MALLOC (sizeof(unsigned int) * b->n_hash);
    for (i = 0; i < b->n_hash; i++)
        a[i] = b->hash_funcs[i](data, size);
    return a;
}


int bloom_contains_hashes(bloom* b, unsigned int* h) {
    int i;
    for (i = 0; i < b->n_hash; i++)
        if (!__bloom_bit_is_set(b, h[i]))
            return 0;
    return 1;
}


void bloom_clear(bloom* b) {
    memset(b->bitmap, 0, DIV8(b->bit_size));
}


void bloom_print(bloom* b) {
    int i, j;
    int byte_size = DIV8(b->bit_size);
    printf("bits: %d bytes: %d\n", b->bit_size, byte_size);
   
    for (i = 0; i < b->n_hash; i++) {
        printf("h = %p\n", b->hash_funcs[i]);
    }

    for (i = 0; i < byte_size; i++) {
        printf("%d \t", i);
        for (j = 7; j >= 0; j--) {
            if (GET_BIT(b->bitmap[i], j) == 0)
                printf("0 ");
            else
                printf("1 ");
        }
        printf("\n");
    }
}


void bloom_union(bloom* b1, bloom* b2) {
    int i, size;
    int *bl1, *bl2;

    bl1 = (int*)b1->bitmap;
    bl2 = (int*)b2->bitmap;
    size = (b1->bit_size / 8) / 4;
    
    assert((b1->bit_size == b2->bit_size));
    assert((b1->n_hash == 1) && (b2->n_hash == 1));
    
    for (i = 0; i < size; i++) {
        bl1[i] |= bl2[i];
    }
}


int bloom_intersect(bloom* b1, bloom* b2) {
    int i, size;
    int *bl1, *bl2;
    
    bl1 = (int*)b1->bitmap;
    bl2 = (int*)b2->bitmap;
    size = (b1->bit_size / 8) / 4;

    assert((b1->bit_size == b2->bit_size));
    assert((b1->n_hash == 1) && (b2->n_hash == 1));
    
    for (i = 0; i < size; i++) {
        if ((bl1[i] & bl2[i]) != 0)
            return 1;
    }
    
    return 0;
}


static void __bloom_set_bit(bloom* b, unsigned int h) {
    int byte_offset, bit_offset;
    h = h % b->bit_size;
    byte_offset = DIV8(h);    
    bit_offset = MOD8(h);
    SET_BIT(b->bitmap[byte_offset], bit_offset);
}


static void __bloom_unset_bit(bloom* b, unsigned int h) {
    int byte_offset, bit_offset;
    h = h % b->bit_size;
    byte_offset = DIV8(h);    
    bit_offset = MOD8(h);
    UNSET_BIT(b->bitmap[byte_offset], bit_offset);
}


static int __bloom_bit_is_set(bloom* b, unsigned int h) {
    int byte_offset, bit_offset;
    h = h % b->bit_size;
    byte_offset = DIV8(h);
    bit_offset = MOD8(h);
    return GET_BIT(b->bitmap[byte_offset], bit_offset);
}
