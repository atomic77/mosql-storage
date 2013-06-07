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

#ifndef _PROTOCOL_H_
#define _PROTOCOL_H_

#include "tapioca.h"

void* protocol_open(const char* address, int port);
void protocol_close(void* ph);
int protocol_node_id(void* ph);
int protocol_client_id(void* p);
int protocol_get(void* ph, void* k, int ksize, void* v, int vsize);
int protocol_put(void* ph, void* k, int ksize, void* v, int vsize);
int protocol_commit(void* ph);
int protocol_rollback(void* ph);

int protocol_mget_int(void* ph, int n, int* keys, int* values);
int protocol_mput_int(void* ph, int n, int* keys, int* values);


int protocol_mget(void* ph, void* k, int ksize);
mget_result* protocol_mget_commit(void* ph);

int protocol_mput(void* ph, void* k, int ksize, void* v, int vsize);
int protocol_mput_commit(void* ph);

int protocol_mget_put(void* ph, void* k, int ksize, void* v, int vsize);
int protocol_mget_put_commit(void* ph);
int protocol_mput_commit_retry(void* ph, int times);

#endif
