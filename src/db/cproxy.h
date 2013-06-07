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

#ifndef _CPROXY_H_
#define _CPROXY_H_

#include "dsmDB_priv.h"
#include <event2/event.h>

typedef void(*cproxy_commit_cb)(tr_id*, int);

int cproxy_init(const char* paxos_config, struct event_base *base);
int cproxy_submit(char* value, size_t size, cproxy_commit_cb cb);
int cproxy_submit_join(int id, char* address, int port);
int cproxy_current_st();
void cproxy_cleanup();

#endif /* _CPROXY_H_ */
