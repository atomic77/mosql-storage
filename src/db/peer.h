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

#ifndef _PEER_H_
#define _PEER_H_

struct peer;
typedef int (*consistent_hash)(unsigned int);


void peer_add(int id, char* address, int port);
void peer_add_cache_node(int id, char* address, int port);
struct peer* peer_get(int id);
struct peer* peer_get_by_info(const char* address, int port);
int peer_count();
char* peer_address(struct peer* p);
int peer_port(struct peer* p);
int peer_node_type(struct peer* p);
void peer_add_recnode(int id, char* address, int port);
struct peer* peer_get_recnode(int id);
consistent_hash peer_get_default_hash();

// TODO To be removed?
struct peer* peer_for_hash(unsigned int h);
int peer_id_for_hash(unsigned int h);

#endif
