#include "opt.h"
#include "util.h"
#include "stats.h"

#include <vector>
#include <cstdio>
#include <cstdlib>
#include <climits>
#include <algorithm>


using namespace std;


struct stats {
	vector<int> tps;
	struct statistics* tps_stats;
};


static int percentiles[] = {50};


static const char* sample_file = NULL;
static int start = 0;
static int end = 0;
static int k = 0;
static int header = 1;
static int help = 0;


static struct option options[] = {	
	{'s', "start", &start, int_opt},
	{'e', "end"  , &end  , int_opt},
	{'n', "no header", &header, fla_opt},
	{'k', "divide input by 1k", &k, fla_opt},
	{'h', "Help" , &help , fla_opt},
	{0, 0, 0, int_opt}
};


static float k_input(float n) {
  return n / (float)1000;
}


static int read_line(FILE* f, float* n) {
	return fscanf(f, "%f", n);
}


static void add_stats(struct stats* s, float n) {
  if (k) n = k_input(n);
	s->tps.push_back(n);
	stats_push(s->tps_stats, n);
}


static void print_percentile_header() {
	printf("#");
	for (int i = 0; i < (int)(sizeof(percentiles) / sizeof(int)); i++) {
		printf("%7d%%", percentiles[i]);
	}
	printf("%8s", "stdev-");
	printf("%8s", "avg");
	printf("%8s", "stdev+");
	printf("\n");
}


static void print_percentiles(struct stats* s) {
	float p, avg, stdev;
	sort(s->tps.begin(), s->tps.end());

	if (header)
		print_percentile_header();
	
	printf(" ");	
	for (int i = 0; i < (int)(sizeof(percentiles) / sizeof(int)); i++) {
		p = ((float)percentiles[i] / 100.0) * ((float)s->tps.size()-1.0);
		printf("%8d", s->tps[p]);
	}
	
	avg = stats_avg(s->tps_stats);
	stdev = stats_stdev(s->tps_stats);
	
	printf("%11d", (int)(avg - stdev));
	printf("%11d", (int)avg);
	printf("%11d", (int)(avg + stdev));
	printf("\n");
}


static void do_stats(FILE* f) {
	int rv;
	int line = 1;
	int fields = 1;
	struct stats s;
	float n;
	
	s.tps_stats = stats_new();
	
	while ((rv = read_line(f, &n)) == fields) {
		if (line > start)
			add_stats(&s, n);
		line++;
		if (line > end && end > 0) {
			rv = EOF;
			break;
		}
	}

	if (rv != EOF) {
		printf("Read error at line %d\n", line);
		return;
	}

	print_percentiles(&s);
	
	stats_free(s.tps_stats);
}


static void usage(char const* progname) {
	char optstr[128];
	get_options_string(options, optstr);
	printf("Usage: %s [-%s] [file ...]\n", progname, optstr);
	print_options(options);
	exit(1);
}


int main(int argc, char * const argv[]) {
	int rv;
	FILE* f;
	
	rv = get_options(options, argc, argv);
	if (rv == -1 || help) usage(argv[0]);
	
	if (rv == argc) {
		f = stdin;
		do_stats(f);
	} else {
		while (rv < argc) {
			sample_file = argv[rv];
			f = fopen(sample_file, "r");
			if (f == NULL) { perror("fopen"); return 1; }
			do_stats(f);
			fclose(f);
			rv++;
		}
	}
	
	return 0;
}
