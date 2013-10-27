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

#include <evpaxos/config.h>

#include "config_reader.h"
#include "dsmDB_priv.h"
#include "validation.h"
#include "queue.h"
#include "socket_util.h"
#include "util.h"
#include "peer.h"


#include <paxos.h>
#include <libpaxos/libpaxos_messages.h>
//#include "libpaxos_messages.h"
#include "msg.h"
#include "tapiocadb.h"


#include <stdlib.h>
#include <signal.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdlib.h>

#include <event2/listener.h>
#include <event2/event.h>
#include <event2/bufferevent.h>
#include <event2/buffer.h>

static unsigned int max_batch_size = 0;
static unsigned long long int received_bytes = 0;

static unsigned long long int submitted_bytes = 0;
static unsigned int submitted_buffers = 0;
static unsigned int submitted_full    = 0;
static unsigned int submitted_timeout = 0;
static unsigned int aborted_tx = 0;
static unsigned int committed_tx = 0;
static unsigned int max_tx_size = 0;
static unsigned int max_buffer_size = 0;
static unsigned int max_buffer_count = 0;
static time_t first_submitted;
static time_t last_submitted;
static int max_count = 0;

static unsigned char *read_buffer;


static void print_stats();

#define MAX_COMMAND_SIZE 256 * 1024
static struct event_base *base;
static struct bufferevent *acc_bev;
static struct event *timeout_ev;
struct evpaxos_config *conf;

static int bytes_left;
static struct timeval timeout_tv;


// Submit committed transaction to paxos
static int submit_transaction(tr_submit_msg *t) {
	int rv, written;
	paxos_msg pm;
	struct evbuffer* payload = evbuffer_new();
	written = add_validation_state(payload);
	assert(written == evbuffer_get_length(payload));
	pm.data_size = evbuffer_get_length(payload);
	pm.type = submit;
	LOG(VRB,("Submitting tx of size %d\n", pm.data_size));
	bufferevent_write(acc_bev, &pm, sizeof(paxos_msg));
	bufferevent_write_buffer(acc_bev, payload);
	evbuffer_free(payload);
}

// Since we are no longer batching tx due to the use of TCP we can
// submit right away
static int validate(tr_submit_msg* t) {
	int commit = 1;
	int bytes_used;
	
	if (validate_transaction(t)) {
	    bytes_used = sizeof(tr_id) + t->writeset_size;
		committed_tx++;

	} else {
	    bytes_used = sizeof(tr_id);
	    aborted_tx++;
		commit = 0;
	}
	submit_transaction(t);
	reset_validation_buffer();
	
	return commit;
}


static void validate_buffer(char* buffer, size_t size) {
	tr_submit_msg* t;
	int count = 0, idx = 0;
	
	while (idx < size) {
		count++;
		t = (tr_submit_msg*)&buffer[idx];
		idx += TR_SUBMIT_MSG_SIZE(t);
		validate(t); 
	}
	
	if (count > max_count)
		max_count = count;
	
	free(buffer);
}


static void handle_join_message(join_msg *jmsg) {
	
	int rv, written;
	paxos_msg pm;
	struct evbuffer* payload = evbuffer_new();
	jmsg->ST = validation_ST();
	
	reconf_msg rmsg;
	rmsg.type = RECONFIG;
	//rmsg.nodes = NumberOfNodes + sizeof(len(pending_list))
	rmsg.ST = validation_ST();
	
	// TODO Implement
	// add_node_config(rmsg.data);
	
	pm.data_size = sizeof(join_msg);
	pm.type = submit;
	evbuffer_add(payload,jmsg, sizeof(join_msg));
	bufferevent_write(acc_bev, &pm, sizeof(paxos_msg));
	bufferevent_write_buffer(acc_bev, payload);
	evbuffer_free(payload);
}

static void handle_reconfig(reconf_msg *rmsg) {
	// stub
	// TODO
	// Find nodes in rmsg that were in pending list, remove them
	// peer_add/sync to update current state
	
}

struct request {
	short type;
	char data[0];
};

static void signal_int(int sig) {
	print_stats();
	exit(0);
}

static void on_socket_event(struct bufferevent *bev, short ev, void *arg) {
    if (ev & BEV_EVENT_CONNECTED) {
        fprintf(stdout, "cm bufferevent connected\n");
    } else if (ev & BEV_EVENT_ERROR) {
        int err = EVUTIL_SOCKET_ERROR();

        fprintf(stderr, "cm: error %d (%s)\n",
            err, evutil_socket_error_to_string(err));

    }
}



static void
on_read(struct bufferevent* bev, void* arg)
{
	size_t len, dlen;
	char *buf;
	struct evbuffer* b;
	short type;
	join_msg jmsg;
	tr_submit_msg tmsg;
	len =0;
	memset(&tmsg, 0, sizeof(tr_submit_msg));
	
	b = bufferevent_get_input(bev);
	while ((len = evbuffer_get_length(b)) >= sizeof(short)) 
	{
		
		//if(len < sizeof(short)) return;
		evbuffer_copyout(b, &type, sizeof(short));
		
		switch (type) {
			case TRANSACTION_SUBMIT:
				if(len < sizeof(tr_submit_msg)) return;
				
				evbuffer_copyout(b, &tmsg, sizeof(tr_submit_msg));
				dlen = TR_SUBMIT_MSG_SIZE((&(tmsg)));
				if (len < dlen) return;
				
				evbuffer_remove(b, read_buffer, dlen);
				validate((tr_submit_msg *) read_buffer);
				break;
			case NODE_JOIN:
				if(len < sizeof(join_msg)) return;
				
				evbuffer_remove(b, &jmsg, sizeof(join_msg));
				handle_join_message(&jmsg);
				break;  
			default: 
				printf("dropping unknown message type %d \n",type);
				assert(1);
		}
	}	
			
}

static void
on_bev_error(struct bufferevent *bev, short events, void *arg)
{
	if (events & BEV_EVENT_ERROR)
		perror("Error from bufferevent");
	if (events & (BEV_EVENT_EOF|BEV_EVENT_ERROR))
		bufferevent_free(bev);
}

static void
on_connect(struct evconnlistener *l, evutil_socket_t fd,
	struct sockaddr *addr, int socklen, void *arg)
{
	struct event_base* b = evconnlistener_get_base(l);
	struct bufferevent *bev = bufferevent_socket_new(b, fd, 
		BEV_OPT_CLOSE_ON_FREE);
	bufferevent_setcb(bev, on_read, NULL, on_bev_error, arg);
	bufferevent_enable(bev, EV_READ);
	LOG(VRB, ("accepted connection from...\n"));
}

static void
on_listener_error(struct evconnlistener* l, void* arg)
{
	struct event_base *base = evconnlistener_get_base(l);
	int err = EVUTIL_SOCKET_ERROR();
	fprintf(stderr, "Got an error %d (%s) on the listener. "
		"Shutting down.\n", err, evutil_socket_error_to_string(err));

	event_base_loopexit(base, NULL);
}

struct evconnlistener *
bind_new_listener(struct event_base* b, const char *addr, int port,
 	evconnlistener_cb conn_cb, evconnlistener_errorcb err_cb)
{
	struct evconnlistener *el;
	struct sockaddr_in sin;
	unsigned flags = LEV_OPT_CLOSE_ON_EXEC
		| LEV_OPT_CLOSE_ON_FREE
		| LEV_OPT_REUSEABLE;
	
	memset(&sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = inet_addr(addr);
	sin.sin_port = htons(port);
	el = evconnlistener_new_bind(
		b, conn_cb, NULL, flags, -1, (struct sockaddr*)&sin, sizeof(sin));
	assert(el != NULL);
	evconnlistener_set_error_cb(el, err_cb);
	
	return el;
}

static struct bufferevent*
proposer_connect(struct event_base* b, struct sockaddr_in* a) {
	struct bufferevent* bev;
	bev = bufferevent_socket_new(b, -1, BEV_OPT_CLOSE_ON_FREE);
	bufferevent_enable(bev, EV_WRITE);
	bufferevent_setcb(bev, NULL, NULL, on_socket_event, NULL);
	struct sockaddr* saddr = (struct sockaddr*)a;
	if (bufferevent_socket_connect(bev, saddr, sizeof(*a))) {
		bufferevent_free(bev);
		return NULL;
	}
	printf("Read/write max %ld %ld \n", 
		bufferevent_get_max_to_read(bev), 
		bufferevent_get_max_to_write(bev));

	return bev;
}

static void on_deliver(char* value, size_t size, iid_t iid,
		ballot_t ballot, int prop_id, void *arg) {
	struct header* h = (struct header*)value;
	switch (h->type) {
		case TRANSACTION_SUBMIT:
			// What a tragic waste that every tx submit is going to be delivered here...
			//handle_transaction(value, size);
			break;
		case RECONFIG:
			handle_reconfig((reconf_msg *) value);
			break;
		default:
			printf("handle_request: dropping message of unkown type\n");
	}
}

static void init(const char* tapioca_config, const char* paxos_config) {
	int cm_fd, result;
	pthread_t val_thread;
	int port;
	const char* addr;

	tapioca_init_defaults();
	load_config_file(tapioca_config);	
	init_validation();

	conf = evpaxos_config_read(paxos_config);
	base = event_base_new();

	/* Set up connection to proposer */
	//acc_bev =  proposer_connect(base, &conf->proposers[0]);
	struct sockaddr_in saddr = evpaxos_proposer_address(conf,0);
	acc_bev =  proposer_connect(base, &saddr);
	assert(acc_bev != NULL);
	
	read_buffer = malloc(MAX_COMMAND_SIZE);
	memset(read_buffer, 0, MAX_COMMAND_SIZE);
	
	// The ceritifer will now need to learn configuration change requests
	struct learner *l = evlearner_init(paxos_config, on_deliver, NULL, base);
	assert(l != NULL);
	
	/* Setup local listener */
	struct evconnlistener *el =  bind_new_listener(base, LeaderIP, LeaderPort, 
												   on_connect, on_listener_error);
	gettimeofday(&timeout_tv, NULL);

	event_base_dispatch(base);
}


int main(int argc, char const *argv[]) {
	signal(SIGINT, signal_int);
	
    if (argc != 3) {
        printf("%s <tapioca config> <paxos config>\n", argv[0]);
        exit(1);
    }
	
	init(argv[1], argv[2]);
	
	return 0;
}


void print_stats() {
	int submitted_tx;
	float percent_prevws_conflict = (float)0;
	float percent_conflict = (float)0;
	float abort_percent = (float)0;
    float too_old = (float)0;
    int t = (int)last_submitted - (int)first_submitted;
    
	submitted_tx = aborted_tx + committed_tx;
	if (submitted_tx > 0) {
		abort_percent = (((float)aborted_tx) / submitted_tx) * 100;
	}
	
	if (aborted_tx > 0) {

		percent_conflict = (((float)write_conflict_counter()) / aborted_tx) * 100;
   	 	percent_prevws_conflict = (((float)write_conflict_prevws_counter()) / aborted_tx) * 100;
		too_old = (((float) too_old_counter()) / aborted_tx) * 100;
	}
	
    printf("CM UDP statistics:\n");
	printf("Max count %d\n", max_count);
    printf("Bytes submitted: %lld\n", submitted_bytes);
    printf("Buffers submitted: %d\n", submitted_buffers);
    printf("Submissions timeout: %d\n", submitted_timeout);
    printf("Submissions full: %d\n", submitted_full);
    printf("Transactions submitted: %d\n", submitted_tx);
    printf("Transactions aborted: %d (%.2f%%)\n", aborted_tx, abort_percent);
    printf("Reason: %.2f%% ws_conflict, %.2f%% prev_ws_conflict, %.2f%% too old\n",
        percent_conflict, percent_prevws_conflict, too_old);
    printf("Transactions reordered: %d\n", reorder_counter());
    printf("Maximum transaction size: %d\n", max_tx_size);
	printf("Maximum batch size: %d\n", max_batch_size);
    printf("Maximum buffer size: %d\n", max_buffer_size);
    printf("Maximum buffer count: %d\n", max_buffer_count);
	if (submitted_tx > 0) {
    	printf("Certifications / sec: %d\n", submitted_tx / t);
	    printf("Data out rate: %llu Mbps\n", ((submitted_bytes / (1024*1024)) / t) * 8);
		printf("Data in rate: %llu Mbps\n", ((received_bytes / (1024*1024)) / t) * 8);
	}
    printf("Execution time: %d seconds\n", t);
    printf("Final ST: %d\n", validation_ST());
}
