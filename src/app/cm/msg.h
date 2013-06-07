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

#ifndef _MSG_H_
#define _MSG_H_

#include <sys/socket.h>

struct iovmsg {
	struct msghdr header;
	struct iovec* iov;
	int iov_size;
	int iov_used;
};


struct iovmsg* iovmsg_new();
void iovmsg_free(struct iovmsg* m);
void iovmsg_clear(struct iovmsg* m);
void iovmsg_add_iov(struct iovmsg* m, void* buffer, int size);
struct msghdr* iovmsg_header(struct iovmsg* m);
int iovmsg_size(struct iovmsg* m);

#endif
