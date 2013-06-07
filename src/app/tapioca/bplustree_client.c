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
