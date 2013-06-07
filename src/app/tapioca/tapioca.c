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

#include "opt.h"
#include "tcp.h"
#include "http.h"
#include <stdio.h>
#include <stdlib.h>
#include <tapiocadb.h>


static int recover = 0;
static int join = 0;
static int dump = 0;
static int help = 0;

static int node_id;
static int tcp_port = 5555;
static const char* tapioca_config;
static const char* paxos_config;

static char filename[128];


static struct option options[] = {
	{'r', "recover", &recover, fla_opt},
	{'j', "join", &join, fla_opt},
	{'d', "dump", &dump, fla_opt},
	{'h', "help", &help, fla_opt},
	{0, 0, 0, int_opt}
};


static void usage(char const* progname) {
	char optstr[128];
	get_options_string(options, optstr);
	printf("Usage: %s [-%s] <node id> <tapioca config> <paxos config> [tcp port]\n", progname, optstr);
	print_options(options);
	exit(1);
}


int main(int argc, char* const argv[]) {
	int rv;

	rv = get_options(options, argc, argv);
	if (rv == -1 || help) usage(argv[0]);
	
	if (argc >= rv + 3) {
		node_id = atoi(argv[rv++]);
		tapioca_config = argv[rv++];
		paxos_config = argv[rv++];
	} else {
		usage(argv[0]);
	}
	
	if (argc > rv) {
		tcp_port = atoi(argv[rv++]);
	}
	
	tapioca_init(node_id, tapioca_config, paxos_config);
	if (dump) {
		sprintf(filename, "/tmp/tapioca-store-%d.bin", node_id);
		tapioca_dump_store_at_exit(filename);
	}
	
	if (join) {
		printf("Join host %s:%d (%d) \n", argv[6], atoi(argv[7]), tcp_port);
        tapioca_add_node(node_id, argv[6], atoi(argv[7]));
        tcp_init(tcp_port);
        tapioca_start_and_join();
	} else {
		if (tcp_init(tcp_port) == -1) {
			printf("tcp_init(): failed\n");
			return 1;
		}
		if (recover)
			printf("Recover\n");
		
		tapioca_start(recover);
	}
	
	return 0;
}
