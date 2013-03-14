#ifndef _VSET_H_
#define _VSET_H_

#ifdef __cplusplus
extern "C" {
#endif


/**
	A vset is a set of versioned values. A vset supports the following
	operations:
		- add:	adds a new version to a vset
		- get:	retrieve a given version

	A vset ensures: 
		- at most StorageMaxOldVersions values are stored in a vset
		- sm-consistency: when getting version v, the vset returns an item
		  with version v' s.t. v' <= v
*/


#include "dsmDB_priv.h"


/**
	Use the #define below to switch between different implementations
 	of vset. Three implementations are currently supported:

	USE_VSET_LIST
	USE_VSET_ARRAY
	USE_VSET_ARRAY_SORTED
	USE_VSET_ARRAY_CACHE
*/
#define USE_VSET_ARRAY_CACHE


/**
	Forward declaration of vset.
*/
typedef struct vset_t* vset;


/**
	Creates a new vset. 
*/
vset vset_new();


/**
	Destroys the given vset.
*/
void vset_free(vset s);


/**
	Adds a new val v, to the vset (created its own copy of v).
	Returns 0, if the call to vset_add() succeded.
*/
int vset_add(vset s, val* v);


/**
	Returns a value such that its version is smaller or equal to v. 
	Returns NULL if no such value exists.
*/
val* vset_get(vset s, int v);


/**
	Returns the total amount of bytes allocated by this vset.
*/
int vset_allocated_bytes(vset s);


/**
	Returns the number of versions stored by vset s.
*/
int vset_count(vset s);


#ifdef __cplusplus
}
#endif

#endif
