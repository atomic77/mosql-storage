#ifndef _PEER_H_
#define _PEER_H_

struct peer;
typedef int (*consistent_hash)(unsigned int);


void peer_add(int id, char* address, int port);
void peer_add_cache_node(int id, char* address, int port);
struct peer* peer_get(int id);
int peer_count();
char* peer_address(struct peer* p);
int peer_port(struct peer* p);
void peer_add_recnode(int id, char* address, int port);
struct peer* peer_get_recnode(int id);
consistent_hash peer_get_default_hash();

// TODO To be removed?
struct peer* peer_for_hash(unsigned int h);
int peer_id_for_hash(unsigned int h);

#endif
