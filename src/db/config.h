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
	We have no legitimate reason to limit this to 64k now but there
	are many places where this implicit assumption still exists and
	stack allocations that should be moved onto the heap
*/
#define MAX_TRANSACTION_SIZE PAXOS_MAX_VALUE_SIZE



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
