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

#ifndef _TCP_PRIV_H_
#define _TCP_PRIV_H_

typedef struct tcp_handle {
	int node_id;
	int client_id;
	tcp_connection* c;
	struct evbuffer* b;
	int mget_buffer_count;
	struct evbuffer* mget_buffer;
	int mput_buffer_count;
	struct evbuffer* mput_buffer;
} tcp_handle;

#define CHECK_BUFFER_SIZE(b, s) \
 	assert(evbuffer_get_length(msg) == (s+2*sizeof(int)))
 	
struct evbuffer* buffer_with_header(int size, int type);
void buffer_add_data_with_size(struct evbuffer* msg, void* data, int size);


#endif
