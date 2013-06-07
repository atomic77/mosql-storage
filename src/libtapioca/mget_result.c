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

#include "mget_result.h"
#include <stdlib.h>
#include <event2/event.h>
#include <event2/buffer.h>


int mget_result_consume(mget_result* res, void* v) {
    int bytes;
    if (res->count <= 0) 
        return -1;
    evbuffer_remove(res->buffer, &bytes, sizeof(int));
	evbuffer_remove(res->buffer, v, bytes);
    res->count--;
    return bytes;
}


int mget_result_count(mget_result* res) {
    return res->count;
}


void mget_result_free(mget_result* res) {
    evbuffer_free(res->buffer);
    free(res);
}
