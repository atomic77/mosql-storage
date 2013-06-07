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

#ifndef _REMOTE_H_
#define _REMOTE_H_

#include "dsmDB_priv.h"
#include <evpaxos/config_reader.h>
#include <event2/event.h>


typedef void(*sm_get_cb)(key*, val*, void*);


int remote_init(struct config *lp_config, struct event_base *base);

int remote_cleanup();

void remote_get(key* k, int version, sm_get_cb cb, void* arg);

void remote_start_recovery();

void remote_print_stats();

#endif /*_REMOTE_H_ */
