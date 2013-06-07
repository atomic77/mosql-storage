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

#include "tcp_connection.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/queue.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>


static int
setnonblock(int fd) {
	int flags;
	flags = fcntl(fd, F_GETFL);
	if (flags < 0)
		return flags;
	flags |= O_NONBLOCK;
	if (fcntl(fd, F_SETFL, flags) < 0)
		return -1;
	return 0;
}


static int
setreuse(int fd) {
	int yes = 1;
	return setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));
}


static void
set_addr(struct sockaddr_in* addr, const char* s_addr, int port) {
	memset(addr, 0, sizeof(addr));
	addr->sin_family = AF_INET;
	addr->sin_addr.s_addr = inet_addr(s_addr);
	addr->sin_port = htons(port);
}


static int 
socket_connect(const char* address, int port) {
	int s, rv;
	struct sockaddr_in addr;
	s = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (s == -1) return -1;
	rv = setreuse(s);
	if (rv == -1) return -1;
	set_addr(&addr, address, port);
	rv = connect(s, (struct sockaddr*)&addr, sizeof(struct sockaddr_in));
	if (rv == -1) return -1;
	rv = setnonblock(s);
	if (rv == -1) return -1;
	return s;
}


static int
message_incomplete(struct evbuffer* b) {
	int bsize;
	int msize;
	bsize = evbuffer_get_length(b);
	if (bsize < sizeof(int))
		return 1;
	evbuffer_copyout(b, &msize, sizeof(int));
	if (bsize < (2*sizeof(int) + msize))
		return 1;
	return 0;
}


static void
on_read(struct bufferevent *bev, void *arg) {
	int size;
	struct evbuffer* input;
	tcp_connection* c = (tcp_connection*)arg;
	
	input = bufferevent_get_input(bev);
	if (message_incomplete(input))
		return;
	
	evbuffer_remove(input, &size, sizeof(int));
    bufferevent_read_buffer(bev, c->out);
	event_base_loopexit(c->base, NULL);
}


static void
on_error(struct bufferevent *bev, short what, void *arg) {
	printf("tcp connection error %d\n", what);
	tcp_connection* c = (tcp_connection*)arg;
	c->error = what;
}


tcp_connection*
tcp_connection_new(const char* address, int port) {
	int rv;
	tcp_connection* c;
	c = malloc(sizeof(tcp_connection));
	if (c == NULL) return NULL;
	c->base = event_base_new();
	c->sock = socket_connect(address, port);
	if (c->sock == -1) return NULL;
	c->buffer_ev = bufferevent_socket_new(c->base, c->sock, BEV_OPT_CLOSE_ON_FREE);
	bufferevent_setcb(c->buffer_ev, on_read, NULL, on_error, c);
	rv = bufferevent_enable(c->buffer_ev, EV_READ);
	if (rv == -1) return NULL;
	c->error = 0;
	return c;
}


void
tcp_connection_free(tcp_connection* c) {
	close(c->sock);
	event_base_free(c->base);
	free(c);
}


int
tcp_write_buffer(tcp_connection* c, struct evbuffer* in, struct evbuffer* out) {
	int rv = 0;
	if (c == NULL) return -1;
	c->out = out;
	evbuffer_drain(out, evbuffer_get_length(out));
	rv = bufferevent_write_buffer(c->buffer_ev, in);
	if (rv == -1) return -1;
	event_base_dispatch(c->base);
	return (int)c->error;
}
