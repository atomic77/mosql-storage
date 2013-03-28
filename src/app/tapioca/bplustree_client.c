/*@
 * Any code related to bptree functionality that we don't want to put in the
 * core tapioca library files
 */
#include "tapioca_btree.h"


void bptree_mget_result_free(bptree_mget_result **bmres)
{
	bptree_mget_result *cur, *next;
	cur = next = *bmres;
	while (next != NULL) {
		next = cur->next;
		free(cur);
		cur = next;
	}
	*bmres = NULL;
}
