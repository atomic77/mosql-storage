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

#ifndef _TAPIOCADB_H_
#define _TAPIOCADB_H_

#ifdef __cplusplus
extern "C" {
#endif

int tapioca_init(const char* tapioca_config, const char* paxos_config);
void tapioca_init_defaults(void);
void tapioca_add_node(int node_id, char* address, int port);
void tapioca_start(int recovery);
void tapioca_start_and_join(void);
void tapioca_dump_store_at_exit(char* path);

#ifdef __cplusplus
}
#endif

#endif
