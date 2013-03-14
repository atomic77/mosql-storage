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
