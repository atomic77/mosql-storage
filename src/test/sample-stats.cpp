#include "opt.h"
#include "util.h"
#include "stats.h"

#include <vector>
#include <cstdio>
#include <cstdlib>
#include <climits>
#include <algorithm>


using namespace std;


struct sample_line {
	float sample_time;
	int real_time;
	int sample_count;
	int tps;
	long long int ulat;
	long long int rlat;
	int requests_count;
};


struct stats {
	vector<int> tps;
	struct statistics* tps_stats;
};


static int percentiles[] = {50};


static const char* sample_file = NULL;
static int start = 0;
static int end   = 0;
static int help = 0;


static struct option options[] = {	
	{'s', "start", &start, int_opt},
	{'e', "end"  , &end  , int_opt},
	{'h', "Help" , &help , fla_opt},
	{0, 0, 0, int_opt}
};


static int read_sample_line(FILE* f, struct sample_line* l) {
	int rv;
    rv = fscanf(f, "%f %d %d %d %lld %lld %d", 
				&l->sample_time,
				&l->real_time,
				&l->sample_count,
				&l->tps,
				&l->ulat,
				&l->rlat,
				&l->requests_count);
	return rv;
}


static void add_stats(struct stats* s, struct sample_line* l) {
	s->tps.push_back(l->tps);
	stats_push(s->tps_stats, l->tps);
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
	
	printf(" ");	
	for (int i = 0; i < (int)(sizeof(percentiles) / sizeof(int)); i++) {
		p = ((float)percentiles[i] / 100.0) * ((float)s->tps.size()-1.0);
		printf("%8d", s->tps[p]);
	}
	
	avg = stats_avg(s->tps_stats);
	stdev = stats_stdev(s->tps_stats);
	
	printf("%8d", (int)(avg - stdev));
	printf("%8d", (int)avg);
	printf("%8d", (int)(avg + stdev));
	printf("\n");
}


static void do_stats(FILE* f) {
	int rv;
	int line = 1;
	int fields = 7;
	struct stats s;
	struct sample_line l;
	
	s.tps_stats = stats_new();
	
	while ((rv = read_sample_line(f, &l)) == fields) {
		line++;
		if (l.sample_time < start)
			continue;
		if (end > 0 && l.sample_time > end)
			continue;
		add_stats(&s, &l);
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
	if (rv == -1 || rv == argc || help) usage(argv[0]);

	print_percentile_header();
	while (rv < argc) {
		sample_file = argv[rv];
		f = fopen(sample_file, "r");
		if (f == NULL) { perror("fopen"); return 1; }
		do_stats(f);
		fclose(f);
		rv++;
	}
	
	return 0;
}
