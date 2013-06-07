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

#include "http.h"
#include <event.h>
#include <stdio.h>
#include <stdlib.h>
#include <tapiocadb.h>


int main(int argc, char const *argv[]) {
	int node_id;
	const char* tapioca_config;
	const char* paxos_config;

	if (argc != 4) {
		printf("Usage: %s <node id> <tapioca config> <paxos config>\n", argv[0]);
		exit(1);
	}
	
	node_id = atoi(argv[1]);
	tapioca_config = argv[2];
	paxos_config = argv[3];
	
	event_init();
	http_init();
	event_dispatch();
	
	return 0;
}
