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

#ifndef _REMOTE_MOCK_H_
#define _REMOTE_MOCK_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "dsmDB_priv.h"

struct remote_mock;

struct remote_mock* remote_mock_new();
void remote_mock_free(struct remote_mock* rm);
val* mock_recover_key(struct remote_mock* rm, key* k);

#ifdef __cplusplus
}
#endif
#endif