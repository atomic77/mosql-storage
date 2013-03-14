#include "tapiocadb.h"
#include "dsmDB_priv.h"
#include "config_reader.h"
#include "sm.h"
#include "cproxy.h"
#include "peer.h"

#include <stdlib.h>
#include <signal.h>
#include <assert.h>
#include <execinfo.h>
#include <sys/time.h>
#include <event2/event.h>
#include <event2/event_compat.h>


static struct event_base* base;
static const char* tapioca_config;
static const char* paxos_config;
static int dump_at_exit = 0;
static char* dump_path;


static void sigint(int sig) {
	struct timeval killtime;
	
	gettimeofday(&killtime, NULL);
    
    printf("\nMain\n");
    printf("------------------------------\n");
	printf("Nodes: %d\n", NumberOfNodes);
    printf("Kill time: %u\n", (unsigned int)killtime.tv_sec);
	printf("Exit signal %d\n", sig);
    printf("------------------------------\n");

	if (dump_at_exit) {
		int version = cproxy_current_st();
		sm_dump_storage(dump_path, version);
	}
    
	if (sig != SIGINT) {
        printf("\nBacktrace\n");
        printf("------------------------------\n");
        size_t size;
        void *array[100];
        char **strings;
        size = backtrace(array, 100);
        strings = backtrace_symbols(array, size);
        int i;
        for (i = 0; i < size; i++)
            printf("%s\n", strings[i]);
        printf("------------------------------\n");
	} else {
	    sm_cleanup();
        cproxy_cleanup();
	}
	
	exit(sig);
}


int tapioca_init(int node_id, const char* tconfig, const char* pconfig) {
	struct timeval now;

	tapioca_init_defaults();
	
	tapioca_config = tconfig;
	paxos_config = pconfig;
	
	signal(SIGINT, sigint);
	signal(SIGSEGV, sigint);
	signal(SIGKILL, sigint);
	signal(SIGTERM, sigint);
	signal(SIGPIPE, SIG_IGN);

	gettimeofday(&now, NULL);
    srandom(now.tv_usec);
	
	#ifndef linux
	setenv("EVENT_NOPOLL", "1", 1);
	setenv("EVENT_NOKQUEUE", "1", 1);
	#endif
	
	NodeID = node_id;
	load_config_file(tapioca_config);
    
	base = event_init();
	// Set two possible priorities: 0 and 1.
	// event_base_priority_init(base, 2);
	
	return 0;
}


void tapioca_init_defaults(void) {
	set_default_global_variables();
}


void tapioca_add_node(int node_id, char* address, int port) {
	peer_add(node_id, address, port);
}


void tapioca_start_and_join(void) {
	int rv;
	struct peer* p;
	rv = sm_init();
	assert(rv >= 0);
	rv = cproxy_init(paxos_config, base);
	assert(rv >= 0);
	p = peer_get(NodeID);
	cproxy_submit_join(NodeID, peer_address(p), peer_port(p));
	event_dispatch();
}


void tapioca_start(int recovery) {
	int rv;
	rv = sm_init();
	assert(rv >= 0);
	rv = cproxy_init(paxos_config, base);
	assert(rv >= 0);
	if (recovery)
		sm_recovery();
	event_dispatch();
}

void tapioca_dump_store_at_exit(char* path) {
	dump_at_exit = 1;
	dump_path = path;
}
