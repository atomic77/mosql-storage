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

#ifndef _SOCKET_UTIL_H_
#define _SOCKET_UTIL_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/types.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <event2/event.h>
#include <event2/bufferevent.h>

int udp_socket();
int udp_socket_connect(char* addr, int port);
void udp_socket_close(int fd);
int udp_bind_fd(int port);

int socket_make_reusable(int fd);
int socket_make_non_block(int fd);
void socket_set_recv_size(int fd, int size);
void socket_set_send_size(int fd, int size);
void socket_set_address(struct sockaddr_in* addr, const char* addrstring, int port);

#ifdef __cplusplus
}
#endif
#endif
