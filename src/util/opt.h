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

#ifndef _OPT_H_
#define _OPT_H_

#ifdef __cplusplus
extern "C" {
#endif

enum optiontype {
	str_opt,
	int_opt,
	fla_opt,
};

struct option {
	char  flag;
	const char* desc;
	void* pvar;
	enum optiontype type;
};

int get_options(struct option* opt, int argc, char * const argv[]);
void get_options_string(struct option* opt, char* str);
void print_options(struct option* opt);

#ifdef __cplusplus
}
#endif

#endif
