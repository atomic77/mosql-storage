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

#ifndef _RECOVERY_LEARNER_MSG_H_
#define _RECOVERY_LEARNER_MSG_H_

#define REC_KEY_MSG 	0
#define REC_KEY_REPLY	101

/*
	TODO needs ST???
*/
typedef struct rec_key_msg_t {
	int type;
	int req_id;
	int node_id;
	int ksize;
	char data[0];
} rec_key_msg;


typedef struct rec_key_reply_t {
	int type;
	int req_id;
	int size;
	int version;
	char data[0];
} rec_key_reply;

#endif
