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

#ifndef _SM_H_
#define _SM_H_

#include "dsmDB_priv.h"
#include <event2/bufferevent.h>
#include <evpaxos/config.h>

int sm_init(struct evpaxos_config *lp_config, struct event_base *base);

int sm_cleanup();

// Returns NULL if value is not local, TM must retry later
// Return a null_val (a val of size 0) if k does not exist
// otherwise returns a valid val*
val* sm_get(key* k, int version);

int sm_put(key* k, val* v);

void sm_recovery();

void sm_dump_storage(char* path, int version);

#endif /* _SM_H_ */
