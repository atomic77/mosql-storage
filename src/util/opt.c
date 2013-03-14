#include "opt.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>


static void getoptstr(struct option* opt, char* str) {
	int i = 0;
	int s = 0;
	while (opt[i].flag != 0) {
		str[s] = opt[i].flag;
		if ((opt[i].type == str_opt) || 
			 opt[i].type == int_opt) {
			s++;
			str[s] = ':';
		}
		i++;
		s++;
	}
	str[s] = 0;
}


static void set_option(struct option* opt, char c) {
	int i = 0;
	while (opt[i].flag != 0) {
		if (opt[i].flag != c) {
			i++;
			continue;
		}

		switch (opt[i].type) {
			case int_opt:
				*((int*)opt[i].pvar) = atoi(optarg);
				break;
			case str_opt:
				*((char**)opt[i].pvar) = optarg;
				break;
			case fla_opt:
				if (*((int*)opt[i].pvar) == 1)
					*((int*)opt[i].pvar) = 0;
				else
					*((int*)opt[i].pvar) = 1; 
				break;
		}
		i++;
	}
}


int get_options(struct option* opt, int argc, char * const argv[]) {
	char c;
	char opt_str[128];
	getoptstr(opt, opt_str);
	while ((c = getopt(argc, argv, opt_str)) != -1) {
		switch (c) {
		case '?':
			return -1;
			break;
		default:
			set_option(opt, c);
		}
	}
	return optind;
}


void get_options_string(struct option* opt, char* str) {
	int i = 0;
	while (opt[i].flag != 0) {
		str[i] = opt[i].flag;
		i++;
	}
	str[i] = 0;
}


void print_options(struct option* opt) {
	int i = 0;
	while (opt[i].flag != 0) {
		if (opt[i].type == int_opt)
			printf("  -%c [int] : %s\n", opt[i].flag, opt[i].desc);
		if (opt[i].type == str_opt)
			printf("  -%c [str] : %s\n", opt[i].flag, opt[i].desc);
		if (opt[i].type == fla_opt)
			printf("  -%c       : %s\n", opt[i].flag, opt[i].desc);
		i++;
	}
}
