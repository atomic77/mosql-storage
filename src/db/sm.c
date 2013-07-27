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

#include "sm.h"
#include "hash.h"
#include "storage.h"
#include "remote.h"
#include "peer.h"

#include <event.h>
#include <stdlib.h>
#include <assert.h>


static int recovering = 0;
static int32_t cksum = 0;
static struct event gc_timer;
static struct timeval gc_timeval = {0, 100000};


static void print_stats();
static void gc(int fd, short event, void* arg);


int sm_init(struct evpaxos_config *lp_config, struct event_base *base) {
	int rv;
	
	rv = remote_init(lp_config, base);
	assert(rv >= 0);
	
	rv = storage_init();
	assert(rv >= 0);
    
	evtimer_set(&gc_timer, gc, NULL);
	event_add(&gc_timer, &gc_timeval);
    return 1;
}


int sm_cleanup() {
	print_stats();    
    storage_free();
    remote_cleanup();
    return 1;
}


/*
    Returns NULL if value is not local, TM must retry later.
    Return a null_val (a val of size 0) if k does not exist.
    Otherwise returns a valid val*
*/
val* sm_get(key* k, int version) {
	val* v;
	// unsigned int i, h;
    
    // Lookup local storage
    if ((v = storage_get(k, version)) != NULL)
        return v;
	
	// Either k does not exist; or, if we are recovering
	// it is not recovered yet.
	if (recovering) {
		// This will force a remote get.
		return NULL;
	}
	
	// Does k belongs to this node?
	// h = joat_hash(k->data, k->size) % NumberOfNodes;
	// for (i = 0; i < REP_DEGREE; i++) {
	// 	if (((h+i) % NumberOfNodes) == NodeID) {
	// 		return val_new(NULL, 0);
	// 	}
	// }
    return NULL;
}


int sm_put(key* k, val* v) {
	int local = 0;
	if (peer_id_for_hash(joat_hash(k->data, k->size)) == NodeID)
		local = 1;
    return storage_put(k, v, local, 0);
}


void sm_recovery() {
	recovering = 1;
    remote_start_recovery();
}


static void iter(key* k, val* v, void* arg) {
	size_t rv;
	FILE* fp = (FILE*)arg;
	// Put will consist of keylen, k data, value len, value data
	if(fwrite(&k->size, sizeof(int), 1,fp) != 1) goto exception;
	if(fwrite(k->data, k->size, 1,fp) != 1) goto exception;
	if(fwrite(&v->size, sizeof(int), 1,fp) != 1) goto exception;
	if(fwrite(v->data, v->size, 1,fp) != 1) goto exception;
	return;
exception:
	printf("Failed on fwrite!\n");
	exit(-1);
}


void sm_dump_storage(char* path, int version) {
	FILE *fp = fopen(path, "w");
	// Preserve 4 bytes for checksum -- implement if we're sure file is
	// being written correctly
	//	fwrite(&cksum, sizeof(int32_t), 1, fp);
	storage_iterate(version, iter, fp);
	if(fclose(fp) != 0)
	{
		printf("Error closing file!\n");
		exit(-1);
	}
}


/*
    Checks that at least bytes_to_free bytes are available in the storage.
*/
static void collect_garbage(long bytes_to_free) {
    long curr_size = storage_get_current_size(); 
    long free_space = StorageMaxSize - curr_size;
    //Free < 0: return under the limit+min_free_space
    if (free_space <= 0) {
        storage_gc_at_least(-(free_space) + bytes_to_free) ;
    } else if (free_space < bytes_to_free) {
        //Almost full cache
        storage_gc_at_least(bytes_to_free - free_space);
    }
}


static void gc(int fd, short event, void* arg) {
	collect_garbage(StorageMinFreeSize);
	event_add(&gc_timer, &gc_timeval);
}


static void print_stats() {
	long total_size, cache_size;
    long key_count, cached_key_count;
	long val_count, cached_val_count;
    
    total_size = (storage_get_current_size() / 1024) / 1024;
    key_count = storage_key_count();
    val_count = storage_val_count();
    
    collect_garbage(StorageMaxSize);
    
    cache_size = total_size - ((storage_get_current_size() / 1024) / 1024);
    cached_key_count = key_count - storage_key_count();
    cached_val_count = val_count - storage_val_count();
    
    printf("\nSM\n");
    printf("------------------------------\n");
    printf("Total size: %ld MB\n", total_size);
    printf("Cache size: %ld MB\n", cache_size);
    printf("Total keys: %ld\n", key_count);
    printf("Cached keys: %ld\n", cached_key_count);
    printf("Total vals: %ld\n", val_count);
    printf("Cached vals: %ld\n", cached_val_count);
	printf("GC calls: %ld\n", storage_gc_count());
	remote_print_stats();
    printf("------------------------------\n");
}
