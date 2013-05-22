#ifndef _INDEX_H
#define _INDEX_H

#include "dsmDB_priv.h"
#include "storage.h"
#include <libpaxos.h>
#include <libpaxos/paxos_config.h>
#include <unistd.h>
#include <db.h>

typedef struct rlog_t {
//	int log_fd;
	DB *dbp;
	DB_ENV *dbenv;
	DB_TXN* txn;
	size_t record_size;
	DBC *db_cur;
	int cur_enabled;
} rlog;

rlog * rlog_init(const char *path);
iid_t rlog_read(rlog *r, key* k) ;
void rlog_update(rlog *r, key* k, iid_t iid) ;
void rlog_tx_begin(rlog *r);
void rlog_tx_commit(rlog *r);
int rlog_num_keys();

#endif
