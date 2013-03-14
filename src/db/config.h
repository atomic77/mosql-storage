#ifndef _CONFIG_H_
#define _CONFIG_H_

#ifdef __cplusplus
extern "C" {
#endif

/*
    Size of bloom filters used respectively for the elements
    in the buffer and the previous writesets (bit size and 
    number of hash functions).
*/
#define SMALL_BLOOM 1024, 2
#define BIG_BLOOM   16384, 2


/*
    Degree of replication. Number of replicas for
    each data item in dsmdb.
*/
#define REP_DEGREE 1


/*
    Change this from 0 to 4 for different verbosity levels
*/
#define VERBOSITY_LEVEL 0


/*
	The maximum size of a transaction in bytes
*/
#define MAX_TRANSACTION_SIZE 64*1000
//#define MAX_TRANSACTION_SIZE 4*1000 //TEMPORARY!



extern int NodeID;
extern int StorageMaxOldVersions;
extern char* LeaderIP;
extern int LeaderPort;
extern long StorageMaxSize;
extern int StorageMinFreeSize;
extern int MaxPreviousST;
extern int ValidationBufferSize;
extern int NumberOfNodes;
extern int NumberOfCacheNodes;

/*
    Maximum period of time in which the validation buffer 
    must be delivered, even if not full yet. In microseconds.
*/
extern int ValidationDeliverInterval;

void set_default_global_variables(void);


#ifdef __cplusplus
}
#endif

#endif
