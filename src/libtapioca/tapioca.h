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

#ifndef _TAPIOCA_H_
#define _TAPIOCA_H_

#ifdef __cplusplus
extern "C" {
#endif


#include "mget_result.h"

#include <stdint.h>
#include <stddef.h>

typedef void tapioca_handle;


/**
 	Opens a new connection to a tapioca node with given ip address and port.

	@return a new tapioca_handle, NULL on failure.
*/
tapioca_handle* tapioca_open(const char* address, int port);


/**
	Closes tapioca_handle previously opened with tapioca_open()
*/
void tapioca_close(tapioca_handle* th);

/**
	Returns the client id of the tapioca node to which handle	is connected to.

	@param th a opened tapioca_handle
*/

int
tapioca_client_id(tapioca_handle* th);

/**
	Returns the node id of the tapioca node to which handle	is connected to.

	@param th a opened tapioca_handle
*/
int tapioca_node_id(tapioca_handle* th);


/**
	Performs a get operation on the current transaction.

	@param th a opened tapioca_handle
	@param k a pointer to the key to retrieve
	@param ksize the size of the memory area pointed by k
	@param v a pointer to a memory area in which the function can store the
	returned value
	@param vsize the size of the memory area pointed by v
	@return number of bytes written in v, 0 if the key does not exist,
	-1 if the get operation failed
*/
int tapioca_get(tapioca_handle* th, void* k, int ksize, void* v, int vsize);


/**
	Performs a put operation on the current transaction.

	@param th a opened tapioca_handle
	@param k a pointer to the key to put
	@param ksize the size of the memory area pointed by k
	@param v a pointer to the value to put
	@param vsize the size of the memory area pointed by v
	@return 1 if the operations succeeds, -1 otherwise
*/
int tapioca_put(tapioca_handle* th, void* k, int ksize, void* v, int vsize);


/**
	Commits the current transaction.
	
	@param th a opened tapioca_handle
	@return >= 0 if the transaction commits, -1 otherwise
*/
int tapioca_commit(tapioca_handle* th);


/**
	Rolls back the current transaction.
	
	@return 1 if the operations succeeds, -1 otherwise
*/
int tapioca_rollback(tapioca_handle* th);


/**
  Allows to group several get operations and submit them as one.   

  @param th a opened tapioca_handle
	@param k a pointer to the key to retrieve
	@param ksize the size of the memory area pointed by k
	@return >= 0 if the operation succeeds, -1 otherwise
*/
int tapioca_mget(tapioca_handle* th, void* k, int ksize);


/**
  Submits grouped mget operations to tapioca, and commits the current
  transaction.
  
  @param th a opened tapioca_handle
  @return returns an mget_result populated with the returned values,
  or NULL if the operation failed.
*/
mget_result* tapioca_mget_commit(tapioca_handle* th);



int tapioca_mput(tapioca_handle* th, void* k, int ksize, void* v, int vsize);
int tapioca_mput_commit(tapioca_handle* th);
int tapioca_mput_commit_retry(tapioca_handle* th, int times);

int tapioca_mget_put(tapioca_handle* th, void* k, int ksize, void* v, int vsize);
int tapioca_mget_put_commit(tapioca_handle* th);

// These are just wrappers around tapioca_mget() and tapioca_mput()
int tapioca_mget_int(tapioca_handle* th, int n, int* keys, int* values);
int tapioca_mput_int(tapioca_handle* th, int n, int* keys, int* values);


#ifdef __cplusplus
}
#endif

#endif
