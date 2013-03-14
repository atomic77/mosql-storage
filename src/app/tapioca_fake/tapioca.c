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
