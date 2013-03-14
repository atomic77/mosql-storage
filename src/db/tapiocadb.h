#ifndef _TAPIOCADB_H_
#define _TAPIOCADB_H_

#ifdef __cplusplus
extern "C" {
#endif

int tapioca_init(int node_id, const char* tapioca_config, const char* paxos_config);
void tapioca_init_defaults(void);
void tapioca_add_node(int node_id, char* address, int port);
void tapioca_start(int recovery);
void tapioca_start_and_join(void);
void tapioca_dump_store_at_exit(char* path);

#ifdef __cplusplus
}
#endif

#endif
