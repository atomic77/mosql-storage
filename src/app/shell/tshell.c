#include <tapioca.h>
#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <readline/readline.h>
#include <readline/history.h>

#include "util.h"


typedef struct {
	char* name;
	char* desc;
	int   time;
	int (*func) (char*);
} shell_cmd;


static int open_cmd(char* args);
static int close_cmd(char* args);
static int load_cmd(char* args);
static int get_cmd(char* args);
static int gets_cmd(char* args);
static int put_cmd(char* args);
static int mget_cmd(char* args);
static int mput_cmd(char* args);
static int begin_cmd(char* args);
static int commit_cmd(char* args);
static int rollback_cmd(char* args);
static int quit_cmd(char* args);
static int help_cmd(char* args);
static int gethex_cmd(char* args);


#define show_time 1
#define hide_time 0

#define max_value_size 8192


shell_cmd cmds[] = {
	{ "open", "open [ip] [port]\t (defaults: 127.0.0.1 5555)", show_time, open_cmd },
	{ "close", "close", show_time, close_cmd },
	{ "load", "load <path>", show_time, load_cmd },
	{ "get", "get <key>", show_time, get_cmd },
	{ "gets", "same as get, return value is interpreted as a string", show_time, gets_cmd},
	{ "gethex", "same as get, return value is interpreted as hex", show_time, gethex_cmd},
	{ "put", "put <key> <value>", show_time, put_cmd },
	{ "mget", "get <key 1> ... <key n>", show_time, mget_cmd },
	{ "mput", "put <key 1> <value 1> ... <key n> <value n>", show_time, mput_cmd },
	{ "begin", "begin new transaction", hide_time, begin_cmd },
	{ "commit", "commit the current transaction", show_time, commit_cmd },
	{ "rollback", "rollback the current transaction", show_time, rollback_cmd },
	{ "quit", "quit the shell", hide_time, quit_cmd },
	{ "?", "show help message", hide_time, help_cmd },
	{ NULL, NULL, 0, NULL }
};


static int done = 0;
static int autocommit = 1;
static tapioca_handle* th = NULL;


static char* strtrim(char* string) {
	char *s, *t;
	for (s = string; isspace(*s); s++)
		;
	if (*s == 0)
		return s;
	t = s + strlen(s) - 1;
	while (t > s && isspace(*t))
		t--;
	*++t = '\0';
	return s;
}


static int parse_int(char** str, int* out) {
	int n;
	char* end;
	if (*str == NULL)
		return -1;
	n = strtol(*str, &end, 10);
	if (end == *str)
		return -1;
	*out = n;
	*str = end;
	return 0;
}


static int parse_long(char** str, long* out) {
    long n;
	char* end;
	if (*str == NULL)
		return -1;
	n = strtol(*str, &end, 10);
	if (end == *str)
		return -1;
	*out = n;
	*str = end;
	return 0;
}


static int parse_string(char** str, char* out) {
	char *s;
	int n = 0;
	if (*str == NULL)
		return -1;
	for (s = *str; isspace(*s); s++)
		;
	if (*s == 0 || *s != '"')
		return -1;
	s++;
	while (*s != 0 && *s != '"') {
		out[n] = *s++;
		n++;
	}
	if (*s == '"') {	
		*str = (s+1);
		return n;
	}
	return -1;
}


static int parse_argument(char** str, char* out) {
	int rv;
	if (*str == NULL)
		return -1;
	rv = parse_int(str, (int*)out);
	if (rv == 0)
		return sizeof(int);
	rv = parse_string(str, out);
	if (rv > 0)
		return rv;
	return -1;
}


static int parse_ip_and_port(char** str, char* addr, int* port) {
	int rv;
	char* tmp;
	if (str == NULL) return 1;
	tmp = strsep(str, " ");
	if (tmp == NULL) return 1;
	strcpy(addr, tmp);
	if (str == NULL) return 1;
	rv = parse_int(str, port);
	if (rv != 0) return -1;
	return 1;
}


static int open_cmd(char* args) {
	int rv;
	int port = 5555;
	char address[128] = "127.0.0.1";
	
	rv = parse_ip_and_port(&args, address, &port);
	if (rv < 0) {	
		fprintf(stderr, "open: failed to parse arguments\n");
		return -1;
	}
	
	th = tapioca_open(address, port);
	if (th == NULL) {
		fprintf(stderr, "open: %s %d failed\n", address, port);
		return -1;
	}
	return 0;
}


static int close_cmd(char* args) {
	if (th != NULL) {
		tapioca_close(th);
		th = NULL;
	}
	return 0;
}


static int load_cmd(char* args) {
	char path[256];
	int rv,rv2 = -12312, count = 0, dirty = 0;
	int ksize, vsize;
	char k[max_value_size];
	char v[max_value_size];
	
	if (th == NULL) {
		fprintf(stderr, "try `open' before `load'\n");
		return -1;
	}

	parse_argument(&args, path);
	
	FILE* fp = fopen(path, "r");
	while (fread(&ksize,sizeof(int),1,fp) != 0) {
		fread(k,ksize,1,fp);
		fread(&vsize,sizeof(int),1,fp);
		fread(v,vsize,1,fp);
		rv = tapioca_mput(th, k, ksize, v, vsize);
		dirty = 1;
		if (rv > 32*1024) {
			rv2 = tapioca_mput_commit_retry(th, 10);
			if (rv2 < 0) goto err;
			dirty = 0;
		}
		count++;
	}
	
	if(dirty)
	{
		rv = tapioca_mput_commit_retry(th, 10);
		if (rv < 0) goto err;
	}

	fclose(fp);
	printf("%d records loaded\n", count);
	return 0;

err:
	fclose(fp);
	fprintf(stderr, "Error while loading (%d records loaded)\n", count);
	fprintf(stderr, "Current k/v sizes: %d %d rv1, rv2: %d %d \n", 
		ksize, vsize, rv, rv2);
	return -1;
}


static int get_cmd(char* args) {
	int rv, ksize, v;
	char k[max_value_size];
	
	if (th == NULL) {
		fprintf(stderr, "try `open' before `get'\n");
		return -1;
	}
	
	ksize = parse_argument(&args, k);
	if (ksize < 0) {
		fprintf(stderr, "get: argument 1 not a valid argument\n");
		return -1;
	}
	
	rv = tapioca_get(th, &k, ksize, &v, sizeof(int));
	if (rv == -1) {
		fprintf(stderr, "get: failed.\n");
		return -1;
	}
	
	if (strlen(args) > 0) {
		fprintf(stderr, "get: too many arguments\n");
		return -1;
	}
	
	switch (rv) {
		case 0:
			printf("null\n");
			break;
		case sizeof(int):
			printf("%d\n", v);
			break;
		default:
			printf("expecting %lu bytes, retrieved %d bytes\n", sizeof(int), rv);
	}
	
	if (autocommit)
		return commit_cmd(NULL);
	return 0;
}


static int gets_cmd(char* args) {
	int i = 0, rv, ksize;
	char k[max_value_size];
	char v[max_value_size];
	
	if (th == NULL) {
		fprintf(stderr, "try `open' before `gets'\n");
		return -1;
	}
	 
	ksize = parse_argument(&args, k);
	if (ksize < 0) {
		fprintf(stderr, "gets: argument 1 not a valid argument\n");
		return -1;
	}
	
	if (strlen(args) > 0) {
		fprintf(stderr, "put: too many arguments\n");
		return -1;
	}
	
	rv = tapioca_get(th, k, ksize, v, max_value_size);
	if (rv == -1) {
		fprintf(stderr, "gets: failed.\n");
		return -1;
	}
	
	if (rv == 0) {
		printf("null\n");
	} else {
		putchar('"');
		while (i < rv) 
			putchar(v[i++]);
		printf("\"\n");
	}
	
	if (autocommit)
		return commit_cmd(NULL);
	return 0;
}


static int put_cmd(char* args) {
	int rv, ksize, vsize;
	char k[max_value_size];
	char v[max_value_size];
	
	if (th == NULL) {
		fprintf(stderr, "try `open' before `put'\n");
		return -1;
	}
	
	ksize = parse_argument(&args, k);
	if (ksize < 0) {
		fprintf(stderr, "put: argument 1 not a valid argument\n");
		return -1;
	}

	vsize = parse_argument(&args, v);
	if (vsize < 0) {
		fprintf(stderr, "put: argument 2 not a valid argument\n");
		return -1;
	}
	
	if (strlen(args) > 0) {
		fprintf(stderr, "put: too many arguments\n");
		return -1;
	}

	rv = tapioca_put(th, k, ksize, v, vsize);

	if (autocommit)
		return commit_cmd(NULL);
	return 0;
}


static int mget_cmd(char* args) {
	int i=0, rv;
	int keys[64];
	int values[64];
	
	if (th == NULL) {
		fprintf(stderr, "try `open' before `mget'\n");
		return -1;
	}
	
	while (args != NULL && args[0] != '\0' && i < 64) {
		rv = parse_int(&args, &keys[i]);
		if (rv != 0) {
			fprintf(stderr, "mget: argument %d not an integer number\n", i);
			return -1;	
		}
		i++;
	}
	
	if (i == 0) {
		fprintf(stderr, "mget: argument %d not an integer number\n", i);
		return -1;
	}
	
	rv = tapioca_mget_int(th, i, keys, values);
	if (rv < 0) {
		fprintf(stderr, "mget: failed.\n");
		return -1;
	}
	
	int j;
	for (j = 0; j < i; j++)
		printf("%d ", values[j]);
	printf("\n");
	
	printf("committed (%d remote request)\n", rv);
	return 0;
}


static int mput_cmd(char* args) {
	int i = 0;
	int rv, k, v;
	
	if (th == NULL) {
		fprintf(stderr, "try `open' before `mput'\n");
		return -1;
	}
	
	while (args != NULL && args[0] != '\0') {
		rv = parse_int(&args, &k);
		if (rv != 0) {
			fprintf(stderr, "mput: argument %d not an integer number\n", i);
			return -1;
		}
		
		rv = parse_int(&args, &v);
		if (rv != 0) {
			fprintf(stderr, "mput: value for key %d expected\n", k);
			return -1;
		}
		tapioca_mput(th, &k, sizeof(int), &v, sizeof(int));
		i++;
	}
	
	if (i == 0) {
		fprintf(stderr, "mput: too few arguments\n");
		return -1;
	}
	
	rv = tapioca_mput_commit(th);
	if (rv < 0) {
		fprintf(stderr, "mput: failed.\n");
		return -1;
	}
	printf("committed (%d remote requests)\n", rv);
	return 0;
}


static int gethex_cmd(char* args) {
	int i = 0, rv, ksize;
	char k[max_value_size];
	char v[max_value_size];

	if (th == NULL) {
		fprintf(stderr, "try `open' before `gets'\n");
		return -1;
	}

	ksize = parse_argument(&args, k);
	if (ksize < 0) {
		fprintf(stderr, "gets: argument 1 not a valid argument\n");
		return -1;
	}

	if (strlen(args) > 0) {
		fprintf(stderr, "put: too many arguments\n");
		return -1;
	}

	rv = tapioca_get(th, k, ksize, v, max_value_size);
	if (rv == -1) {
		fprintf(stderr, "gets: failed.\n");
		return -1;
	}

	if (rv == 0) {
		printf("null\n");
	} else {
		printf("0x ");
		while (i < rv)
			printf("%2X ", v[i++]);
		printf(" \n");
	}

	if (autocommit)
		return commit_cmd(NULL);
	return 0;
}


static int begin_cmd(char* args) {
	autocommit = 0;
	return 0;
}


static int commit_cmd(char* args) {
	int rv;
	
	if (th == NULL) {
		fprintf(stderr, "try `open' before `commit'\n");
		return -1;
	}
	
	rv = tapioca_commit(th);
	if (rv < 0)
		printf("aborted\n");
	else
		printf("committed (%d remote requests)\n", rv);
	
	if (!autocommit)
		autocommit = 1;
	
	return 0;
}


static int rollback_cmd(char* args) {
	int rv;
	
	if (th == NULL) {
		fprintf(stderr, "try `open' before `rollback'");
		return -1;
	}
	
	rv = tapioca_rollback(th);
	if (rv < 0)
		printf("failed\n");
		
	if (!autocommit)
		autocommit = 1;
	
	return 0;
}


static int quit_cmd(char* args) {
	close_cmd(args);
	done = 1;
	return 0;
}


static int help_cmd(char* args) {
	int i = 0;
	while (cmds[i].name != NULL) {
		printf ("%-10s\t%s\n", cmds[i].name, cmds[i].desc);
		i++;
	}
	return 0;
}


static shell_cmd* find_command(char* name) {
	int i;
	for (i = 0; cmds[i].name; i++)
		if (strcmp(name, cmds[i].name) == 0)
			return &cmds[i];
	return NULL;
}


static int execute_line(char* line) {
	int rv, us;
	char *cmdstr;
	shell_cmd* cmd;
	struct timeval start, end;

	cmdstr = strsep(&line, " ");
	cmd = find_command(cmdstr);
	if (cmd == NULL) {
		fprintf (stderr, "%s: command not found.\n", cmdstr);
		return -1;
	}
	
	gettimeofday(&start, NULL);
	rv = cmd->func(line);
	gettimeofday(&end, NULL);
	
	us = timeval_diff(&start, &end);
	if ((rv == 0) && (cmd->time))
		printf("took %.2f ms\n", (us / (double)1000));
	return rv;
}


static char* command_generator(const char* text, int state) {
	char* name;
	static int list_index, len;
	
	if (!state) {
		list_index = 0;
		len = strlen(text);
	}
	
	while ((name = cmds[list_index].name) != NULL) {
		list_index++;
		if (strncmp(name, text, len) == 0)
			return strdup(name);
	}
	
	return NULL;
}


static char** tshell_completion(char* text, int start, int end) {
	char** matches = NULL;
	if (start == 0)
		matches = rl_completion_matches(text, command_generator);
	return matches;
}


static void readline_init() {
	rl_readline_name = "tshell";
	rl_attempted_completion_function = (CPPFunction*)tshell_completion;
}


static char* prompt() {
	static char p[64];
	if (th == NULL) {
		sprintf(p, "> ");
	} else {
		sprintf(p, "tapioca@%d > ", tapioca_node_id(th));
	}
	return p;
}


int main(int argc, char *argv[]) {
	char* s, *line;
	
	readline_init();
	printf("enter `?' for help.\n");
	
	while (!done) {
		line = readline(prompt());
		if (!line) break;
		
		s = strtrim(line);
		if (*s) {
			add_history(s);
			execute_line(s);
		}
		free(line);
	}
	return 0;
}
