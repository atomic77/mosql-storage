#ifndef _CONFIG_READER_H_
#define _CONFIG_READER_H_

#include "dsmDB_priv.h"

int load_config_file(const char * path);
node_info * get_node_info(int node_id);

#endif /* _CONFIG_READER_H_ */
