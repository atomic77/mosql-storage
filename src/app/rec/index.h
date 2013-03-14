#ifndef _INDEX_H
#define _INDEX_H

#include "dsmDB_priv.h"
#include "paxos_config.h"
#include <unistd.h>
#include <db.h>
#include "libpaxos.h"
//#include "libpaxos_messages.h"
#include "storage.h"

typedef struct rlog_t {
//	int log_fd;
	DB *dbp;
	DB_ENV *dbenv;
	size_t record_size;
	DBC *db_cur;
	int cur_enabled;
} rlog;

rlog * rlog_init(const char *path);
iid_t rlog_read(rlog *r, key* k) ;
void rlog_update(rlog *r, key* k, iid_t iid) ;
int rlog_num_keys();

#endif
