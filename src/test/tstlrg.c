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

#include "tapioca.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

int main(int argc , char **argv){
    tapioca_handle *th;
	int i;
    char* v;
    char* check;
	if (argc < 4) 
	{
		printf("%s <val size> <commit freq> <total keys>\n", argv[0]);
		exit(1);
	}
    int vsize = atoi(argv[1]);
	int commit_freq = atoi(argv[2]);
    int iterations = atoi(argv[3]);
	int commits = 0;
	
    int rv;
    th = tapioca_open("127.0.0.1",5555);

    v = (char*)malloc(vsize);
    check = (char*)malloc(vsize);

    for (i = 0; i < iterations; i++) {
        memset(v, i, vsize);
        rv = tapioca_put(th, &i, sizeof(int), v, vsize);
        if (!rv) printf("put rv %d \n", rv);
		if (i % commit_freq == 0) {
			rv = tapioca_commit(th);
			if (rv >= 0) commits ++;
		}
                                                    }

    for (i = 0; i < iterations; i++) {
        memset(check, i, vsize);
        rv = tapioca_get(th, &i, sizeof(int), v, vsize);
        if (!rv) printf("get rv %d \n ",rv);
        tapioca_commit(th);
		rv = memcmp(check, v, vsize);
        if (rv != 0) printf("memcmp result %d \n", rv); 
    }

    printf("Finished checking %d keys, %d successful commits\n", 
		   iterations, commits);
    free(v);
    free(check);
}
