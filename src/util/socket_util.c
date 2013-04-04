#include "socket_util.h"
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>

static void set_sock_recv_size(int sock, int size) {
	int rv = -1;
	rv = setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &size, sizeof(int));
	assert(rv == 0);
}


static void set_sock_send_size(int sock, int size) {
	int rv = -1;
	rv = setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &size, sizeof(int));
	assert(rv == 0);
}


static int socket_add_attribute(int sock, int newflag) {
   	int flags;
	flags = fcntl(sock, F_GETFL);
	if (flags == -1) {
		perror("fcntl failed reading");
        return -1;
	}
	flags |= newflag;
	if (fcntl(sock, F_SETFL, flags) == -1) {
		perror("fcntl failed reading");
        return -1;
	}
    return 0;     
}


int udp_socket() {
	int fd;
	
	fd = socket(AF_INET, SOCK_DGRAM, 0);
	if (fd == -1) {
		perror("socket");
		return fd;
	}

	set_sock_recv_size(fd, 65000);
	set_sock_send_size(fd, 65000);
	
	return fd;
}


void udp_socket_close(int fd) {
	int rv;
	rv = close(fd);
	if (fd == -1) {
		perror("socket");
	}
}


int udp_socket_connect(char* addrstring, int port) {
	int fd;
	struct sockaddr_in addr;
	
	fd = udp_socket();
	socket_set_address(&addr, addrstring, port);
	
	if (connect(fd, (struct sockaddr *)&addr, sizeof(struct sockaddr_in)) < 0) { 
        perror("connect");
    }

	return fd;
}


int udp_bind_fd(int port) {
	int fd, rv;
	struct sockaddr_in addr;
	
	fd = udp_socket();
	
	memset(&addr, '\0', sizeof(struct sockaddr_in));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_ANY);
	addr.sin_port = htons(port);
	
	rv = bind(fd, (struct sockaddr*)&addr, sizeof(struct sockaddr_in));
	if (rv == -1) {
		perror("bind");
		return -1;
	}
	
	return fd;
}


void socket_set_address(struct sockaddr_in* addr, const char* addrstring, int port) {
	memset(addr, '\0', sizeof(struct sockaddr_in));
	addr->sin_family = AF_INET;
	addr->sin_addr.s_addr = inet_addr(addrstring);
	assert(addr->sin_addr.s_addr != INADDR_NONE);
	addr->sin_port = htons(port);
}


int socket_make_reusable(int fd) {
	int yes = 1;
	return setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));
}


int socket_make_non_block(int fd) {
    int rv = -1;
    rv = socket_add_attribute(fd, O_NONBLOCK);
    assert(rv == 0);
    return 0;
}


void socket_set_recv_size(int fd, int size) {
	int rv = -1;
	rv = setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &size, sizeof(int));
	assert(rv == 0);
}


void socket_set_send_size(int fd, int size) {
	int rv = -1;
	rv = setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(int));
	assert(rv == 0);
}
