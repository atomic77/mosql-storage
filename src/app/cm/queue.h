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

#ifndef _QUEUE_H_
#define _QUEUE_H_

#include <stdlib.h>

struct queue;

struct queue* queue_new();
void queue_delete(struct queue* q);
int queue_enq(struct queue* q, void* value, size_t size);
void queue_deq(struct queue* q, void** value, size_t* size);
void queue_deq_timed(struct queue* q, int usec, void** value, size_t* size);

#endif
