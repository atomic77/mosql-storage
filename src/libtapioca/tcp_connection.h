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

#ifndef _TCP_CONNECTION_H_
#define _TCP_CONNECTION_H_

#include <event2/event.h>

typedef struct tcp_connection_t {
	int sock;
	short error;
	struct evbuffer* out;
	struct event_base* base;
	struct bufferevent* buffer_ev;
} tcp_connection;


tcp_connection* tcp_connection_new(const char* address, int port);
void tcp_connection_free(tcp_connection* c);
int tcp_write_buffer(tcp_connection* c, struct evbuffer* in, struct evbuffer* out);

#endif
