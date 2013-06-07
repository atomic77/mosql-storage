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

#include "tapioca_btree.h"
#include "protocol_btree.h"
#include <stdlib.h>


tapioca_bptree_id
tapioca_bptree_initialize_bpt_session_no_commit(tapioca_handle *th,
	tapioca_bptree_id bpt_id, enum bptree_open_flags open_flags, uint32_t execution_id)
{
	int rv = protocol_bptree_initialize_bpt_session_no_commit(th,
			   bpt_id, open_flags, execution_id);

	if (rv == 1) return bpt_id;
	return -1;
}

tapioca_bptree_id
tapioca_bptree_initialize_bpt_session(tapioca_handle *th,
		tapioca_bptree_id bpt_id, enum bptree_open_flags open_flags)
{
	int rv = protocol_bptree_initialize_bpt_session(th, bpt_id, open_flags);
	if (rv == 1) return bpt_id;
	return -1;
}

int tapioca_bptree_set_num_fields(
		tapioca_handle *th,tapioca_bptree_id tbpt_id,  int16_t num_fields)
{
	return protocol_bptree_set_num_fields(th,tbpt_id, num_fields);
}

int tapioca_bptree_set_field_info(tapioca_handle *th,
		tapioca_bptree_id tbpt_id, int16_t field_num,
		int16_t field_sz, enum bptree_field_comparator comparator)
{
	return
		protocol_bptree_set_field_info(th,tbpt_id,field_num,field_sz,comparator);
}


int tapioca_bptree_insert(tapioca_handle *th, tapioca_bptree_id tbpt_id,
	void *k,int32_t ksize,void *v, int32_t vsize, enum bptree_insert_flags insert_flags)
{
	return protocol_bptree_insert(th,tbpt_id,k,ksize,v,vsize,insert_flags);

}

int tapioca_bptree_update(tapioca_handle *th, tapioca_bptree_id tbpt_id,
	void *k,int32_t ksize,void *v, int32_t vsize)
{
	return protocol_bptree_update(th,tbpt_id,k,ksize,v,vsize);
}

int tapioca_bptree_search(tapioca_handle *th, tapioca_bptree_id tbpt_id,
	void *k,int32_t ksize,void *v, int32_t *vsize)
{
	return protocol_bptree_search(th,tbpt_id,k,ksize,v,vsize);
}

int tapioca_bptree_index_first(tapioca_handle *th, tapioca_bptree_id tbpt_id,
	void *k,int32_t *ksize,void *v, int32_t *vsize)
{
	return protocol_bptree_index_first(th,tbpt_id,k,ksize,v,vsize);
}

int tapioca_bptree_index_next(tapioca_handle *th, tapioca_bptree_id tbpt_id,
	void *k,int32_t *ksize,void *v, int32_t *vsize)
{
	return protocol_bptree_index_next(th,tbpt_id,k,ksize,v,vsize);
}

int tapioca_bptree_index_next_mget(tapioca_handle *th,tapioca_bptree_id tbpt_id,
		bptree_mget_result **bmres, int16_t *rows)
{
	return protocol_bptree_index_next_mget(th,tbpt_id,bmres, rows);
}

int tapioca_bptree_index_first_no_key(tapioca_handle *th,
		tapioca_bptree_id tbpt_id)
{
	return protocol_bptree_index_first_no_key(th, tbpt_id);
}

int tapioca_bptree_debug(tapioca_handle *th, tapioca_bptree_id tbpt_id,
		enum bptree_debug_option debug_opt)
{
	return protocol_bptree_debug(th, tbpt_id, debug_opt);
}

int tapioca_bptree_delete(tapioca_handle *th, tapioca_bptree_id tbpt_id,
	void *k,int32_t ksize,void *v, int32_t vsize)
{
	return protocol_bptree_delete(th,tbpt_id,k,ksize,v,vsize);
}
