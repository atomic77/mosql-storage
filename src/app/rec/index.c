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

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <errno.h>
#include "index.h"

#include "hash.h"
#include "khash.h"


struct __attribute__ ((packed)) index_entry {
	off_t off;
	short ksize;
	char kdata[0];
};


static int equal_keys(struct index_entry* k1, struct index_entry* k2);

KHASH_INIT(index, struct index_entry*, char, 0, hash_from_key, equal_keys)

static khash_t(index)* ht;
#define KHASH_INIT_SIZE  10000 // 50000000
#define MEM_CACHE_SIZE (0), (32*1024*1024)

static char read_buffer[PAXOS_MAX_VALUE_SIZE];

void rlog_sync(rlog* r);

/*@
 * Initialize index of tapioca key -> iid
 */
rlog * rlog_init(const char *path) {
    int rv;
	char log_path[512], db_path[512];
    uint32_t open_flags, env_flags;
	rlog *r = (rlog *) malloc(sizeof(rlog));
	memset(r, 0, sizeof(rlog));
    r->cur_enabled = 0;

    // Set up the environment flags for a transactional environment
    env_flags =
      DB_CREATE     |  /* Create the environment if it does not exist */
      DB_RECOVER    |  /* Run normal recovery. */
      DB_INIT_LOCK  |  /* Initialize the locking subsystem */
      DB_INIT_LOG   |  /* Initialize the logging subsystem */
      DB_INIT_TXN   |  /* Initialize the transactional subsystem. This
			* also turns on logging. */
      DB_INIT_MPOOL |  /* Initialize the memory pool (in-memory cache) */
      DB_REGISTER	|
      DB_THREAD ;		  /* Cause the environment to be free-threaded */
      //DB_TXN_WRITE_NOSYNC ;

    open_flags =
    		DB_CREATE;//  | DB_AUTO_COMMIT;
			
	rv = db_env_create(&r->dbenv, 0);
	if (rv != 0) {
		printf("DB_ENV creation failed: %s\n", db_strerror(rv));
	}
	
    assert(rv == 0);

	struct stat sb;
	int dir_exists = (stat(path, &sb) == 0);

	// TODO Include recovery part here for rlog
	
	if (!dir_exists && (mkdir(path, S_IRWXU) != 0)) {
		printf("Failed to create env dir %s: %s\n", path, strerror(errno));
		return NULL;
	} 
    
	//Set the size of the memory cache
	rv = r->dbenv->set_cachesize(r->dbenv, MEM_CACHE_SIZE, 1);
	if (rv != 0) {
		printf("DB_ENV set_cachesize failed: %s\n", db_strerror(rv));
		return NULL;
	}
	rv = r->dbenv->set_flags(r->dbenv, DB_TXN_WRITE_NOSYNC , 1);
	if (rv != 0) {
		printf("DB_ENV set txn no fsync failed: %s\n", db_strerror(rv));
		return NULL;
	}

	
    sprintf(log_path, "%s/", path);
    sprintf(db_path, "%s/rec.db", path);
    rv = r->dbenv->open(r->dbenv, log_path, env_flags, 0);
    if (rv != 0) {
    	fprintf(stderr, "Error opening environment: %s\n", db_strerror(rv));
    	goto err;
    }

    printf("RIDX: Opening up db %s\n", db_path);

    rv = db_create(&r->dbp, r->dbenv, 0);
    if (rv != 0) {
    	fprintf(stderr, "Error opening DB: %s\n", db_strerror(rv));
    	goto err;
    }

    rlog_tx_begin(r);
	
    rv = r->dbp->open(r->dbp,        /* Pointer to the database */
		    r->txn,       /* Txn pointer */
		    db_path,  /* File name */
		    NULL,       /* Logical db name */
		    DB_BTREE,   /* Database type (using btree) */
		    open_flags, /* Open flags */
		    0);         /* File mode. Using defaults */
    if (rv != 0) {
    	r->dbp->err(r->dbp, rv, "RIDX: Database '%s' open failed.", path);
    	return NULL;
    }
    
    rlog_tx_commit(r);
    
	memset(read_buffer, 0, PAXOS_MAX_VALUE_SIZE);
    return r;


err:
    /* Close our database handle, if it was opened. */
    if (r->dbp != NULL) {
    	rv = r->dbp->close(r->dbp, 0);
    	if (rv != 0) {
    		fprintf(stderr, "%s database close failed.\n", db_strerror(rv));
    	}
    }

    /* Close our environment, if it was opened. */
    if (r->dbenv != NULL) {
    	rv = r->dbenv->close(r->dbenv, 0);
    	if (rv != 0) {
    		fprintf(stderr, "environment close failed: %s\n", db_strerror(rv));
    	}
    }

    return NULL;
}


void rlog_tx_begin(rlog *r)
{
	int rv;
	rv = r->dbenv->txn_begin(r->dbenv, NULL, &r->txn, 0);
	assert(rv == 0);	
}

void rlog_tx_commit(rlog *r)
{
	int rv;
	rv = r->txn->commit(r->txn, 0);
	assert(rv == 0);
}


/*@
 * Retrieve iid instance for tapioca key k
 */
iid_t rlog_read(rlog *r, key* k) {
	int rv;
	iid_t inst;
    DBT _k, _v;
    memset(&_k, 0, sizeof(DBT));
    memset(&_v, 0, sizeof(DBT));

    _k.data = k->data;
    _k.size = k->size;
	_v.data = &inst;
	_v.ulen = sizeof(iid_t);
	_v.flags = DB_DBT_USERMEM;

	rv = r->dbp->get(r->dbp, r->txn, &_k, &_v, 0);

	if (rv == DB_NOTFOUND || rv == DB_KEYEMPTY) return 0;
	if (rv != 0)
	{
    	r->dbp->err(r->dbp, rv, " get failed on db with error");
    	return -1;
    }
	assert(_v.size == sizeof(iid_t));
	assert(_v.data != NULL);
	inst = *(iid_t *)_v.data;

	return inst;
}

int rlog_next(rlog *r, iid_t *iid, key *k) 
{
	int rv;
	DBT dbkey, dbdata;
	
	memset(&dbkey, 0, sizeof(DBT));
	memset(&dbdata, 0, sizeof(DBT));
	
	//Data is our buffer
	dbkey.data = read_buffer;
	dbkey.ulen = PAXOS_MAX_VALUE_SIZE;
	dbkey.flags = DB_DBT_USERMEM;

	dbdata.data = iid;
	dbdata.ulen= sizeof(iid_t);
	dbdata.flags = DB_DBT_USERMEM;
    
	
	if (r->db_cur == NULL) {
		rv = r->dbp->cursor(r->dbp, r->txn, &r->db_cur, DB_READ_COMMITTED); 
		if (rv != 0) {
			r->dbp->err(r->dbp, rv, "DB->cursor");
			return -1;
		}
	}
	
	rv = r->db_cur->get(r->db_cur, &dbkey, &dbdata, DB_NEXT);
		
	if (rv ==0) {
		return 1;
	}
	else if (rv == DB_NOTFOUND) 
	{
		r->db_cur->close(r->db_cur);
		r->db_cur = NULL;
		return 0;
	} else {
		r->dbp->err(r->dbp, rv, "DBcursor->get");
		return -1;
	}
	
	
}

void rlog_update(rlog *r, key* k, iid_t iid) {
	int rv;
    DBT _k, _v;
    memset(&_k, 0, sizeof(DBT));
    memset(&_v, 0, sizeof(DBT));

    _k.data = k->data;
    _k.size = k->size;

	_v.data = &iid;
    _v.size = sizeof(iid_t);

	rv = r->dbp->put(r->dbp, r->txn, &_k, &_v, 0);
	assert(rv == 0);

}

// TODO Check if this count needs to be exact -- currently it will be an estim.
int rlog_num_keys(rlog *r) {
	DB_BTREE_STAT *sp;
	r->dbp->stat(r->dbp, NULL, sp, DB_FAST_STAT);
	free(sp);
	return sp->bt_nkeys;
}


static int equal_keys(struct index_entry* k1, struct index_entry* k2) {
	if (k1->ksize == k2->ksize)
		if (memcmp(k1->kdata, k2->kdata, k1->ksize) == 0)
            return 1;
    return 0;
}


void rlog_sync(rlog* r)
{
	r->dbp->sync(r->dbp, 0);
}

void rlog_close(rlog* r)
{
	r->dbp->close(r->dbp, 0);
	r->dbenv->close(r->dbenv, 0);
}

