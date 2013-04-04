#include <libpaxos/config_reader.h>

#include "config_reader.h"
#include "dsmDB_priv.h"
#include "validation.h"
#include "queue.h"
#include "socket_util.h"
#include "util.h"


//#include "libpaxos.h"
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


static void print_stats();

#ifndef MAX_MESSAGE_SIZE
#define RECV_BSIZE 8 * 1024
#define MAX_COMMAND_SIZE RECV_BSIZE
#define MAX_MESSAGE_SIZE RECV_BSIZE
//#define RECV_BSIZE MAX_MESSAGE_SIZE
#endif

static struct event_base *base;
static struct bufferevent *acc_bev;
static struct event *timeout_ev;
struct config *conf;

static int bytes_left;
static struct timeval timeout_tv;


// Submit committed transaction to paxos
static int submit_transaction(tr_submit_msg *t) {
	int rv, written;
	paxos_msg pm;
	pm.data_size = TR_SUBMIT_MSG_SIZE(t) + sizeof(tr_deliver_msg)
			 - sizeof(tr_submit_msg);
	pm.type = submit;
	LOG(VRB,("Submitting tx of size %d\n", pm.data_size));
	bufferevent_write(acc_bev, &pm, sizeof(paxos_msg));
	written = add_validation_state(acc_bev);
	assert(written == pm.data_size);
}

static int transaction_fits_in_buffer(tr_submit_msg* m) {
    int bytes_if_commits = m->writeset_size + sizeof(tr_id);
    return (bytes_left > (sizeof(tr_deliver_msg) + bytes_if_commits));
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


static void handle_join_message(char* buffer, size_t size) {
	join_msg* m;
	assert(0 == 0xDEADBEEF);
	// FIXME Implement me

/*	lp_message_header* paxos_header;
	submit_cmd_msg* paxos_submit_cmd;
	m = (join_msg*)buffer;
	m->ST = validation_ST();
	
	paxos_header = malloc(1024);
	paxos_header->type = client_submit;
	paxos_submit_cmd = (submit_cmd_msg*)paxos_header->data;

	paxos_submit_cmd->cmd_size = size;
	paxos_header->size = size + sizeof(submit_cmd_msg);
	memcpy(paxos_submit_cmd->cmd_value, buffer, size);
	
	send(leader_fd, paxos_header, paxos_header->size + sizeof(paxos_header), 0);
	
	free(paxos_header);
*/
}


struct request {
	short type;
	char data[0];
};

static void handle_request(struct request *req, char* buffer, size_t size) {
	switch (req->type) {
		case TRANSACTION_SUBMIT:
			validate_buffer(buffer, size);
			break;
		case NODE_JOIN:
			handle_join_message(buffer, size);
			break;
		default:
			printf("handle_request: dropping message of unkown type\n");
	}
}


// static void cm_loop(int fd) {
// 	int n;
// 	char* b;
// 	socklen_t addr_len;
// 	struct sockaddr_in addr;
// 
// 	addr_len = sizeof(struct sockaddr_in);
// 
// 	// TODO Here we need to move to a libevent based listener
// 	while (1) {
// 		b = malloc(RECV_BSIZE + sizeof(int));
// 		n = recvfrom(fd,
// 					 b,
// 					 RECV_BSIZE,
// 					 0,
// 					 (struct sockaddr*)&addr,
// 					 &addr_len);
// 
// 		if (n == -1) {
// 			perror("recvfrom");
// 			continue;
// 		}
// 
// 		received_bytes += n;
// 		if (n > max_batch_size)
// 			max_batch_size = n;
// 
// 		handle_request(b, n);
// 	}
// }

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
	size_t len;
	paxos_msg msg;
	char *buf;
	struct evbuffer* b;
	struct request *req;
	
	b = bufferevent_get_input(bev);
	
	len = evbuffer_get_length(b);
	assert (len > 0 && len < 1024 * 1024); // some arbitrary large # for now
	buf = malloc(len);

	evbuffer_remove(b, buf, len); 

	// The header of the submit_msg is basically a struct request anyway
	req = (struct request *) buf; 
	
	// As we do no batching all requests are of size 1 now
	handle_request(req, buf, 1);
	
	//free(buf); // done in the caller
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
	struct tcp_receiver* r = arg;
	struct event_base* b = evconnlistener_get_base(l);
	struct bufferevent *bev = bufferevent_socket_new(b, fd, 
		BEV_OPT_CLOSE_ON_FREE);
	bufferevent_setcb(bev, on_read, NULL, on_bev_error, arg);
	bufferevent_enable(bev, EV_READ);
// 	carray_push_back(r->bevs, bev);
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
bind_new_listener(struct event_base* b, address* a,
 	evconnlistener_cb conn_cb, evconnlistener_errorcb err_cb)
{
	struct evconnlistener *el;
	struct sockaddr_in sin;
	unsigned flags = LEV_OPT_CLOSE_ON_EXEC
		| LEV_OPT_CLOSE_ON_FREE
		| LEV_OPT_REUSEABLE;
	
	memset(&sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = inet_addr(a->address_string);
	sin.sin_port = htons(a->port);
	el = evconnlistener_new_bind(
		b, conn_cb, NULL, flags, -1, (struct sockaddr*)&sin, sizeof(sin));
	assert(el != NULL);
	evconnlistener_set_error_cb(el, err_cb);
// 	r->bevs = carray_new(10);
	
	return el;
}

static struct bufferevent*
proposer_connect(struct event_base* b, address* a) {
	struct sockaddr_in sin;
	struct bufferevent* bev;
	
	LOG(VRB,("Connecting to proposer %s : %d\n",
	                a->address_string, a->port));
	memset(&sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = inet_addr(a->address_string);
	sin.sin_port = htons(a->port);
	
	bev = bufferevent_socket_new(b, -1, BEV_OPT_CLOSE_ON_FREE);
	bufferevent_enable(bev, EV_WRITE);
	bufferevent_setcb(bev, NULL, NULL, on_socket_event, NULL);
	struct sockaddr* saddr = (struct sockaddr*)&sin;
	if (bufferevent_socket_connect(bev, saddr, sizeof(sin)) < 0) {
		bufferevent_free(bev);
		return NULL;
	}

	return bev;
}

static void init(const char* tapioca_config, const char* paxos_config) {
	int cm_fd, result;
	pthread_t val_thread;
	int port;
	const char* addr;

	tapioca_init_defaults();
	load_config_file(tapioca_config);	
	init_validation();

	conf = read_config(paxos_config);
	base = event_base_new();

	/* Set up connection to proposer */
	acc_bev =  proposer_connect(base, &conf->proposers[0]);
	assert(acc_bev != NULL);
	
	bytes_left = MAX_COMMAND_SIZE;
	
	/* Setup local listener */
	address a;
	a.address_string = LeaderIP;
	a.port = LeaderPort;
	struct evconnlistener *el =  bind_new_listener(base, &a, on_connect, on_listener_error);
// 	cm_fd = udp_bind_fd(LeaderPort);
	gettimeofday(&timeout_tv, NULL);
	
// 	cm_loop(cm_fd);
	
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
