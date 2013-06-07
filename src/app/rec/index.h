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
int rlog_next(rlog *r, iid_t *iid, key *k) ;
void rlog_close(rlog* r);

#endif
