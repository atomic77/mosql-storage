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

#ifndef _VALIDATION_H_
#define _VALIDATION_H_

#include "dsmDB_priv.h"
#include "msg.h"
#include <event2/buffer.h>

void init_validation();

int is_validation_buf_full();

int validate_transaction(tr_submit_msg* t);

void reset_validation_buffer();

int validation_cleanup();

int validated_count();

int reorder_counter();

int validation_ST();

int write_conflict_counter();

int write_conflict_prevws_counter();

int too_old_counter();

int validate_phase1(tr_submit_msg* t);
int validate_phase2(tr_submit_msg* t, int commit);

int add_validation_state(struct evbuffer *b);

#endif /* _VALIDATION_H_ */
