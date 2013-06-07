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

#include "stats.h"
#include <math.h>
#include <stdlib.h>


struct statistics {
	int n;
	double min;
	double max;
	double avg;
	double var;
};


struct statistics* 
stats_new() {
	struct statistics* stats;
	stats = malloc(sizeof(struct statistics));
	if (stats == NULL) return NULL;
	stats_clear(stats);
	return stats;
}


void 
stats_free(struct statistics* stats) {
	free(stats);
}


void
stats_clear(struct statistics* stats) {
	stats->n = 0;
	stats->avg = 0.0;
	stats->var = 0.0;
	stats->min = 0.0;
	stats->max = 0.0;
}


void
stats_push(struct statistics* stats, double x) {
	stats->n++;
	if (stats->n == 1) {
		stats->avg = x;
		stats->min = x;
		stats->max = x;
	} else {
		double old_avg;
		old_avg = stats->avg;
		stats->avg = stats->avg + ((x - stats->avg) / stats->n);
		stats->var = stats->var + ((x - old_avg) * (x - stats->avg));
		if (x > stats->max) {
			stats->max = x;
		} else if (x < stats->min) {
			stats->min = x;
		}
	}
}


int 
stats_count(struct statistics* stats) {
	return stats->n;
}


double
stats_min(struct statistics* stats) {
	return stats->min;
}


double
stats_max(struct statistics* stats) {
	return stats->max;
}


double
stats_avg(struct statistics* stats) {
	return stats->avg;
}


double
stats_var(struct statistics* stats) {
	return stats->var / (stats->n-1);
}


double
stats_stdev(struct statistics* stats) {
	return sqrt(stats_var(stats));
}
