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
#include <getopt.h>
#include <string.h>


static int recover = 0;
static int join = 0;
static int dump = 0;
static int help = 0;

static int node_id;
static int tcp_port = 5555;
static const char* tapioca_config;
static const char* paxos_config;

static char filename[128];


/*
static struct option options[] = {
	{'r', "recover", &recover, fla_opt},
	{'j', "join", &join, fla_opt},
	{'d', "dump", &dump, fla_opt},
	{'h', "help", &help, fla_opt},
	{0, 0, 0, int_opt}
};
*/


struct option long_options[] =
{
		/* These options set a flag. */
		{"node-type", 		required_argument, 0, 'n'},
		{"ip-address",   	required_argument, 0, 'i'},
		{"port",	required_argument,       0, 'p'},
		{"dump-state",  	no_argument,       0, 'd'},
		{"paxos-config",	required_argument,       0, 'c'},
		{"storage-config",  	required_argument,       0, 's'},
		{"recover",  	no_argument,       0, 'r'},
		{"help",  			no_argument,       0, 'h'},
		{0, 0, 0, 0}
};

const char *short_opts = "n:i:p:dc:s:rh";

void print_usage()
{

	struct option opt = long_options[0];
	int i = 0;
	fprintf(stderr, "Command line options:\n");
	while (1)
	{
		if (opt.name == 0)
			break;
		fprintf(stderr, "\t--%s , -%c \n", opt.name, opt.val);
		i++;
		opt = long_options[i];
	}
	fprintf(stderr, "Node type: %d - regular, %d - cache\n",
		REGULAR_NODE, CACHE_NODE);
}


int main(int argc, char* const argv[]) {
	int rv;
	int opt_idx;
	char ch;
	while ((ch = getopt_long(argc, argv, short_opts, long_options, &opt_idx)) != -1)
	{
		switch (ch)
		{
		case 'n':
			// node type = atoi(optarg);
			// 
			NodeType = atoi(optarg);
			if(NodeType > CACHE_NODE) {
				fprintf(stderr, "Invalid node type\n");
				print_usage();
				exit(1);
			}
			break;
		case 'i':
			strncpy(LocalIpAddress, optarg,17);
			break;
		case 'p':
			LocalPort = atoi(optarg);
			break;
		case 'd':
			dump = 1;
			break;
		case 'c':
			paxos_config = optarg;
			break;
		case 's':
			tapioca_config = optarg;
			break;
		case 'r':
			// no recovery mode yet...
			break;
		case 'h':
			print_usage();
			exit(1);
			break;
		}
	}
	
	tapioca_init(tapioca_config, paxos_config);
	
	if (dump) {
		sprintf(filename, "/tmp/tapioca-store-%s-%d.bin", 
				LocalIpAddress, LocalPort);
		tapioca_dump_store_at_exit(filename);
	}
	
	tcp_init(LocalPort);
	tapioca_start_and_join();
	
	// Old logic
	/*
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
		*/
	return 0;
}
