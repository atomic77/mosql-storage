#include "opt.h"
#include "util.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>


static char* tmp_file = "tmp.txt";
static char* tmp_dir = "/tmp/";
static char* output_file = "out.csv";
static char* input_prefix = "trace-";
static char* log_dir = ".";
static int exclude = 0;
static int sample_period = 1000000;
static int skip_merge = 0;
static int skip_analyze = 0;
static int help = 0;


static struct option options[] = {
	{'f', "tmp file", &tmp_file, str_opt},
	{'t', "tmp directory", &tmp_dir, str_opt},
	{'o', "output file", &output_file, str_opt},
	{'i', "input prefix", &input_prefix, str_opt},
	{'l', "log directory", &log_dir, str_opt},
	{'e', "exclude n samples", &exclude, int_opt},
	{'s', "sample every n microseconds", &sample_period, int_opt},
	{'m', "skip merge phase", &skip_merge, fla_opt},
	{'a', "skip analyze", &skip_analyze, fla_opt},
	{'h', "help", &help, fla_opt},
	{0, 0, 0, 0}
};


static FILE* out;
static FILE* tmp = NULL;

static struct timeval sample_start, tx_start, tx_end;
static int committed;
static int update;

static int time_slot = 1;
static int count = 0;
static int sample_count = 0;
static int sample_reads_count = 0;
static int sample_updates_count = 0;
static int committed_count = 0;
static int requests_count = 0;
static long long int sum_latency_reads = 0;
static long long int sum_latency_updates = 0;


static int read_log_line() {
    int ret_value;
    unsigned int usec_start, usec_end;
    
    ret_value = fscanf(tmp, "%ld, %u, %ld, %u, %d, %d", 
                       &tx_start.tv_sec,
                       &usec_start,
                       &tx_end.tv_sec,
                       &usec_end,
                       &committed,
					   &update);
                       
    tx_start.tv_usec = usec_start;
    tx_end.tv_usec = usec_end;
    
    return ret_value;
}


static void merge_and_sort_traces() {
    char cmd[255];
    
    printf("Merging client files...\n");
    
	if (!skip_merge) {
	  	sprintf(cmd, "sort -T %s -m %s/%s* > %s/%s", 
			tmp_dir, 
		  	log_dir, 
		  	input_prefix, 
		  	log_dir, 
		  	tmp_file);
	  	
		printf("%s\n", cmd);
	
	  	if (system(cmd) != 0) {
	    	printf("Command \"%s\" failed.\n", cmd);
	    	exit(1);
	  	}
	}
    
    sprintf(cmd, "%s/%s", log_dir, tmp_file);
    if ((tmp = fopen(cmd, "r")) == NULL) {
        perror("fopen");
        exit(1);
    }
    
    sprintf(cmd, "%s/%s", log_dir, output_file);
    if ((out = fopen(cmd, "w")) == NULL) {
        perror("fopen");
        exit(1);
    }
}


static void print_output_line() {
    float x;
    float r;
    long long int avg_lat_reads = 0;
    long long int avg_lat_updates = 0;
	
    if (sample_reads_count > 0)
      avg_lat_reads = sum_latency_reads / sample_reads_count;

    if (sample_updates_count > 0)
      avg_lat_updates = sum_latency_updates / sample_updates_count;
    
    r = (float)1000000 / (float)sample_period;
    x = (float)time_slot / r;
    
    fprintf(out, "%f %u %d %d %lld %lld %d\n", 
            x, 
            (unsigned int)sample_start.tv_sec,
            sample_count,
            (int)(committed_count * r),
            avg_lat_updates,
			avg_lat_reads,
			requests_count);
}


static void analyze_log_files() {
	int rv;
	int line = 0;
	
	rv = read_log_line();
	if (rv == EOF) {
		printf("File Empty\n");
		return;
	}
    
    printf("Resampling with period %d usec...\n", sample_period);
        
    sample_start.tv_sec = tx_start.tv_sec;
    sample_start.tv_usec = tx_start.tv_usec;
    
    while (1) {
		line++;
		if (rv != 6) { // 6 is the number of "fields" matched by fscanf
		  printf("Read error at line %d\n", line);
		  break;
		}
        if (timeval_diff(&sample_start, &tx_start) < sample_period) {
            count++;
            sample_count++;

			if (update) {
				sample_updates_count++;
            	sum_latency_updates += timeval_diff(&tx_start, &tx_end);
			} else {
				sample_reads_count++;
				sum_latency_reads += timeval_diff(&tx_start, &tx_end);
			}
				
            if (committed >= 0) {
                committed_count++;
				requests_count += committed;
			}
        } else {
            if (exclude > time_slot) {
                exclude--;
            } else {
                print_output_line();
                time_slot++;
            }
            
            sample_count = 0;
			sample_reads_count = 0;
			sample_updates_count = 0;
            committed_count = 0;
            sum_latency_reads = 0;
			sum_latency_updates = 0;
			requests_count = 0;
            
            timeval_add(&sample_start, sample_period);
        }
        
		rv = read_log_line();
		if (rv == EOF)
			break;
    }
    
    print_output_line();
}


static void usage(char const* progname) {
	printf("Usage: %s\n", progname);
	print_options(options);
	exit(1);
}


static void print_cmd_line(int argc, char* argv[]) {
	int i;
	for (i = 0; i < argc; i++)
		printf("%s ", argv[i]);
	printf("\n");
}


int main (int argc, char *argv[]) {
	int rv;
	rv = get_options(options, argc, argv);
	if (rv == -1 || help) usage(argv[0]);
	print_cmd_line(argc, argv);

    merge_and_sort_traces();
	if (!skip_analyze)
    	analyze_log_files();
    
    return 0;
}
