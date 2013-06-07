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

#ifndef _STATS_H_
#define _STATS_H_

#ifdef __cplusplus
extern "C" {
#endif

struct statistics;

struct statistics* stats_new();
void stats_free(struct statistics* stats);
void stats_push(struct statistics* stats, double x);
void stats_clear(struct statistics* stats);
int  stats_count(struct statistics* stats);
double stats_min(struct statistics* stats);
double stats_max(struct statistics* stats);
double stats_avg(struct statistics* stats);
double stats_var(struct statistics* stats);
double stats_stdev(struct statistics* stats);

#ifdef __cplusplus
}
#endif

#endif
