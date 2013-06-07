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

#ifndef _STORAGE_H_
#define _STORAGE_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "dsmDB_priv.h"
#include "peer.h"

int storage_init(void);

int storage_init2(consistent_hash chash);

void storage_free();

val* storage_get(key* k, int max_ver);

int storage_put(key* k, val* v, int local, int force_cache);

long storage_get_current_size();

long storage_key_count();

long storage_val_count();

long storage_gc_count();

void storage_gc_at_least(int bytes);

int storage_iterate(int version, void (iter)(key*, val*, void*), void* arg);

void storage_gc_start();

void storage_gc_stop();

#ifdef __cplusplus
}
#endif

#endif /* _STORAGE_H_ */
