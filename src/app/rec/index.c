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


//static struct index_entry* index_entry_new(key* k, off_t off);
//static unsigned int hash_from_key(struct index_entry* k1);
static int equal_keys(struct index_entry* k1, struct index_entry* k2);


KHASH_INIT(index, struct index_entry*, char, 0, hash_from_key, equal_keys)


static khash_t(index)* ht;
#define KHASH_INIT_SIZE  10000 // 50000000


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
//      DB_AUTO_COMMIT |
      DB_TXN_NOSYNC ;
//      DB_THREAD;       /* Cause the environment to be free-threaded */

    open_flags =
    		DB_CREATE |
    		DB_AUTO_COMMIT;
			
	rv = db_env_create(&r->dbenv, 0);
	if (rv != 0) {
		printf("DB_ENV creation failed: %s\n", db_strerror(rv));
	}
	
    assert(rv == 0);

	struct stat sb;
	int dir_exists = (stat(path, &sb) == 0);
// 	int db_exists = (stat(db_file_path, &sb) == 0);

	// TODO Include recovery part here for rlog
	
	if (!dir_exists && (mkdir(path, S_IRWXU) != 0)) {
		printf("Failed to create env dir %s: %s\n", path, strerror(errno));
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

//    rv = r->dbp->set_cachesize(r->dbp, 0, 50*1024*1024, 0);
    rv = r->dbp->open(r->dbp,        /* Pointer to the database */
		    NULL,       /* Txn pointer */
		    db_path,  /* File name */
		    NULL,       /* Logical db name */
		    DB_BTREE,   /* Database type (using btree) */
		    open_flags, /* Open flags */
		    0);         /* File mode. Using defaults */
    if (rv != 0) {
    	r->dbp->err(r->dbp, rv, "RIDX: Database '%s' open failed.", path);
    	return NULL;
    }
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

	rv = r->dbp->get(r->dbp, NULL, &_k, &_v, 0);

	if (rv == DB_NOTFOUND) return 0;
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


void rlog_update(rlog *r, key* k, iid_t iid) {
	int rv;
	iid_t inst;
    DBT _k, _v;
    memset(&_k, 0, sizeof(DBT));
    memset(&_v, 0, sizeof(DBT));

    _k.data = k->data;
    _k.size = k->size;

	_v.data = &iid;
    _v.size = sizeof(iid_t);

//    printf("RLOG: Putting key size %d to iid %d\n", k->size, iid);
	rv = r->dbp->put(r->dbp, 0, &_k, &_v, 0);
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
	rlog_sync(r);
	r->dbp->close(r->dbp, 0);
}

