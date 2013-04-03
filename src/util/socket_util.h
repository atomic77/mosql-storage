#ifndef _SOCKET_UTIL_H_
#define _SOCKET_UTIL_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/types.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <event2/event.h>
#include <event2/bufferevent.h>

int udp_socket();
int udp_socket_connect(char* addr, int port);
void udp_socket_close(int fd);
int udp_bind_fd(int port);

int socket_make_reusable(int fd);
int socket_make_non_block(int fd);
void socket_set_recv_size(int fd, int size);
void socket_set_send_size(int fd, int size);
void socket_set_address(struct sockaddr_in* addr, const char* addrstring, int port);

/** Convenience method to create a buffered connection to a host.
 *  The caller should then do bufferevent_setcb to configure the callbacks
	@param b a valid event_base
	@param a an address to connect to
	@return a bufferevent handle, or NULL on error */
struct bufferevent* 
ev_buffered_connect(struct event_base* b, const char *address_string, int port,
					short int event_type);

#ifdef __cplusplus
}
#endif
#endif
