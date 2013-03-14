#include "remote_mock.h"
#include "socket_util.h"
#include "recovery_learner_msg.h"
#include "hash.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <event2/event.h>


struct remote_mock {
	val* v;
	int send_sock;
	int recv_sock;
	char buffer[1024];
	int buffer_size;
	struct event* ev_read;
	struct event_base* base;
};


static int my_id = 111;
static int my_port = 11111;
static char* my_address = "127.0.0.1";


static void mock_join(struct remote_mock* rm) {
	int fd;
	char buffer[1024];
	join_msg* msg = (join_msg*)buffer;
	msg->type = NODE_JOIN;
	msg->node_id = my_id;
	msg->port = my_port;
	strcpy(msg->address, my_address);
	fd = udp_socket_connect("127.0.0.1", 8888);
	send(fd, buffer, sizeof(join_msg), 0);
	close(fd);
}


struct remote_mock* remote_mock_new() {
	struct remote_mock* rm;
	rm = malloc(sizeof(struct remote_mock));
	rm->base = event_base_new();
	mock_join(rm);
	return rm;
}


void remote_mock_free(struct remote_mock* rm) {
	event_base_free(rm->base);
}


static void rec_key_msg_for_key(key* k, char* buffer, int* size) {
	rec_key_msg* msg;
	msg = (rec_key_msg*)buffer;
	msg->type = REC_KEY_MSG;
	msg->req_id = 1;
	msg->node_id = my_id;
	msg->ksize = k->size;
	memcpy(msg->data, k->data, k->size);
	*size = sizeof(rec_key_msg) + k->size;
}


static void handle_rec_key(int fd, short ev, void* arg) {
	char buffer[1024];
	struct remote_mock* rm = (struct remote_mock*)arg;
	rec_key_reply* msg = (rec_key_reply*)buffer;
	recv(fd, buffer, 1024, 0);
	rm->v = versioned_val_new(msg->data, msg->size, msg->version);
}


val* mock_recover_key(struct remote_mock* rm, key* k) {
	int port = 12345;
	unsigned int h = joat_hash(k->data, k->size);
		
	rec_key_msg_for_key(k, rm->buffer, &rm->buffer_size);
	rm->recv_sock = udp_bind_fd(my_port);
	socket_make_reusable(rm->recv_sock);
	socket_make_non_block(rm->recv_sock);
	if (h % 2 != 0)
		port = 12346;
	rm->send_sock = udp_socket_connect("127.0.0.1", port);
	send(rm->send_sock, rm->buffer, rm->buffer_size, 0);	
	rm->ev_read = event_new(rm->base, rm->recv_sock, EV_READ, handle_rec_key, rm);
	event_add(rm->ev_read, NULL);
	event_base_dispatch(rm->base);
	close(rm->recv_sock);
	close(rm->send_sock);
	return rm->v;
}
