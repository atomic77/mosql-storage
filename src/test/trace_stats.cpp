#include "opt.h"
#include "util.h"
#include "stats.h"

#include <vector>
#include <cstdio>
#include <cstdlib>
#include <climits>
#include <algorithm>

using namespace std;


struct trace_line {
	struct timeval s;
	struct timeval e;
	int committed;
	int is_update;
	int requests_count; 
};


struct stats {
	int count;
	int committed;
	struct timeval start, end;
	vector<int> latencies;
	struct statistics* latency;
};


static int sample_period = 1000000;
static int percentiles[] = {50};


static const char* trace_file = NULL;
static int start = 0;
static int end = 0;
static int extended_stats = 0;
static int only_updates = 0;
static int only_reads = 0;
static int help = 0;


static struct option options[] = {
	{'s', "Start at sample time n", &start, int_opt},
	{'e', "End at sample time n", &end, int_opt},
	{'x', "Print extended stats", &extended_stats, fla_opt },
	{'r', "Print stats only for reads", &only_reads, fla_opt },
	{'u', "Print stats only for updates", &only_updates, fla_opt },
	{'h', "Help", &help, fla_opt},
	{0, 0, 0, int_opt}
};


static int read_trace_line(FILE* f, struct trace_line* l) {
	int rv;
    rv = fscanf(f, "%ld, %lu, %ld, %lu, %d, %d", 
				&l->s.tv_sec,
				(long unsigned*)&l->s.tv_usec,
				&l->e.tv_sec,
				(long unsigned*)&l->e.tv_usec,
				&l->committed,
				&l->is_update);
    return rv;
}


static void init_stats(struct stats* s) {
	s->count = 0;
	s->committed = 0;
	s->latency = stats_new();
}


static void add_stats(struct stats* s, struct trace_line* l) {
	int lat;
	s->count++;
	if (s->count == 1) {
		s->start = l->s;
	}
	s->end = l->e;
	if (l->committed >= 0) {
		lat = timeval_diff(&l->s, &l->e);
		s->committed++;
		stats_push(s->latency, lat);
		s->latencies.push_back(lat);
	}
}


static void print_percentile_headers() {
  printf("\n");
	for (int i = 0; i < (int)(sizeof(percentiles) / sizeof(int)); i++) {
		printf("%7d%%", percentiles[i]);
	}
	printf("\n");
}


static void print_percentiles(struct stats* s) {
	float p;
	sort(s->latencies.begin(), s->latencies.end());
	for (int i = 0; i < (int)(sizeof(percentiles) / sizeof(int)); i++) {
		p = ((float)percentiles[i] / 100.0) * ((float)s->latencies.size()-1.0);
		printf("%8.1f", (float)s->latencies[p] / 1000);
	}
	//	printf("\n");
}


static void print_stats(struct stats* s) {
	int aborted;
	unsigned long int exec_time;
	aborted = s->count - s->committed;
	exec_time = s->end.tv_sec - s->start.tv_sec;
	if (extended_stats) {
		printf("tx           : %d\n", s->count);
		printf("tx commit    : %d\n", s->committed);
		printf("abort rate   : %.2f\n", ((float)aborted / (float)s->count) * 100.0);
		printf("time (sec)   : %lu\n", exec_time);
		printf("tx/sec       : %lu\n", s->committed / exec_time);	
		printf("latency (ms)\n");
		printf("%8s%8s%8s%8s\n", "min", "avg", "stdev", "max");
		printf("%8.1f", stats_min(s->latency) / 1000.0);
		printf("%8.1f", stats_avg(s->latency) / 1000.0);
		printf("%8.1f", stats_stdev(s->latency) / 1000.0);
		printf("%8.1f\n", stats_max(s->latency) / 1000.0);
		//print_percentile_headers();
	} else {
	}
	
	float avg = stats_avg(s->latency) / 1000.0;
	float stdev = stats_stdev(s->latency) / 1000.0;
	
	print_percentiles(s);			  
	printf("%8.1f", avg - stdev);
	printf("%8.1f", avg);
	printf("%8.1f", avg + stdev);
	printf("\n");
}


static void do_stats(FILE* f) {
	int rv;
	int line = 1;
	int fields = 6;
	struct stats rstats;
	struct stats ustats;
	struct trace_line l;
	int sample_time = 0;
	static struct timeval sample_start;
	
	init_stats(&rstats);
	init_stats(&ustats);
	
	while ((rv = read_trace_line(f, &l)) == fields) {
		if (sample_time == 0) {
			sample_start.tv_sec = l.s.tv_sec;
		    sample_start.tv_usec = l.s.tv_usec;
			sample_time++;
		}
		
		if (timeval_diff(&sample_start, &l.s) > sample_period) {
			timeval_add(&sample_start, sample_period);
			sample_time++;
		}
		
		if (sample_time < start) {
			continue;
		}
		
		if (end > 0 && sample_time > end) {
			continue;
		}
		
		if (l.is_update)
			add_stats(&ustats, &l);
		else
			add_stats(&rstats, &l);

		line++;
	}

	if (rv != EOF) {
		printf("Read error at line %d\n", line);
		return;
	}
	
	if (ustats.count > 0 && !only_reads) {
	  if (extended_stats)
	    printf("Update stats\n");
	  print_stats(&ustats);
	}
	
	if (rstats.count > 0 && !only_updates) {
	  if (extended_stats)
	    printf("\nRead stats\n");
	  print_stats(&rstats);		
	}
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
		
	while (rv < argc) {
		trace_file = argv[rv];
		f = fopen(trace_file, "r");
		if (f == NULL) { perror("fopen"); return 1; }
		do_stats(f);
		fclose(f);
		rv++;
	}
	
	return 0;
}
