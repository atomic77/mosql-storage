/*
    Copyright (C) 2013 University of Lugano

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

#include "bptree_unittest.h"
#include <time.h>

struct thread_config
{
	tapioca_bptree_id seed;
	int dbug;
	int keys;
	int thread_id;
	int start_key;
} ;


class BptreeConcurrencyTest : public BptreeTestBase {
protected:
	int num_threads;
	int dbug, seed;
	struct timespec tms, tmend;
	long runtime;
	double druntime;
	pthread_t *threads;
	struct thread_config *s;
	
	void SetUp() {
		BptreeTestBase::SetUp();
		num_threads = 4;
		keys = 200;

		dbug = 0;
		seed = 1010;
		srand(seed);

		threads = (pthread_t *) malloc(sizeof(pthread_t) * num_threads);

		s = (struct thread_config *) 
			malloc(sizeof(struct thread_config) * num_threads * 2);

	}

};

void *thr_tapioca_bptree_traversal_test(void *data)
{
	// Assume that we've inserted k keys into index; pick a random place to
	// search and then verify that we index_next the correct # of records
	int i, j, k, v, n, r, rv, ksize, vsize;
	struct timespec tms, tmend;
	double druntime;
	long runtime;
	int* arr;
	tapioca_handle *th;
	struct thread_config *s = (struct thread_config *) data;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;

	th = tapioca_open("127.0.0.1", 5555);
	tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, s->seed,
			BPTREE_OPEN_CREATE_IF_NOT_EXISTS);
	tapioca_bptree_set_num_fields(th,tbpt_id, 1);
	tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);

	if (th == NULL)
	{
		printf("Failed to connect to tapioca\n");
		return NULL;
	}

	char kk[10] = "aaaaaaaaa";
	char vv[10] = "cccc";
	char k2[10];
	char v2[10];
	arr = (int *) malloc(s->keys * sizeof(int));
	for (i = 0; i < s->keys; i++)
		arr[i] = i + s->start_key;
	n = s->keys;

	printf("THR_ID %d: Searching then traversing keys...\n", s->thread_id);
	for (i = 0; i < s->keys; i++)
		arr[i] = i + s->start_key;
	n = s->keys;
	clock_gettime(CLOCK_MONOTONIC, &tms);
	for (i = 0; i < s->keys; i++)
	{
		r = rand() % (n);
		k = arr[r];
		arr[r] = arr[n - 1];
		n--;
		char *kptr = kk + 1;
		sprintf(kptr, "%08d", k);
		rv = tapioca_bptree_search(th, tbpt_id, kk, 10, vv, &vsize);
		int cnt = 0;
		do
		{
			rv = tapioca_bptree_index_next(th, tbpt_id, k2, &ksize, v2, &vsize);
			if (memcmp(k2, kk, 10) < 0)
			{
				printf("Index_next return out-of-order element: k2, kk %s %s\n",
						k2, kk);
				return 0;
			}
			cnt++;
		} while (rv == 1);

		if (cnt < s->keys - k)
		{
			//printf ("Index_next returned %d keys, expecting %d\n",
			//	cnt, (s->keys -k));
		}
		//if (rv !=1) printf("THR_ID %d: Failed to find %s!\n",s->thread_id,kk);
		tapioca_commit(th);
	}

	clock_gettime(CLOCK_MONOTONIC, &tmend);
	runtime = (tmend.tv_nsec - tms.tv_nsec);
	druntime = (tmend.tv_sec - tms.tv_sec) + (runtime / 1000000000.0);
	printf("THR_ID %d: completed in %.3f seconds, %.2f traversals/s \n",
			s->thread_id, druntime, (double) (s->keys / druntime));

	/// Test index scanning

	//assert(verify_tapioca_bptree_order(th,1));
	//tapioca_commit(th);

	free(arr);
	tapioca_commit(th);
	tapioca_close(th);
	return NULL;

}

void *thr_tapioca_bptree_search_test(void *data)
{
	int i, j, k, v, n, r, rv, vsize;
	struct timespec tms, tmend;
	double druntime;
	long runtime;
	int* arr;
	tapioca_handle *th;
	struct thread_config *s = (struct thread_config *) data;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;

	th = tapioca_open("127.0.0.1", 5555);
	tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, s->seed,
			BPTREE_OPEN_CREATE_IF_NOT_EXISTS);
	tapioca_bptree_set_num_fields(th,tbpt_id, 1);
	tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);

	if (th == NULL)
	{
		printf("Failed to connect to tapioca\n");
		return NULL;
	}

	char kk[10] = "aaaaaaaaa";
	char vv[10] = "cccc";
	arr = (int *) malloc(s->keys * sizeof(int));
	for (i = 0; i < s->keys; i++)
		arr[i] = i + s->start_key;
	n = s->keys;

	printf("THR_ID %d: Searching keys... starting from %d \n", s->thread_id,
			s->start_key);
	for (i = 0; i < s->keys; i++)
		arr[i] = i + s->start_key;
	n = s->keys;
	clock_gettime(CLOCK_MONOTONIC, &tms);
	for (i = 0; i < s->keys; i++)
	{
		r = rand() % (n);
		k = arr[r];
		arr[r] = arr[n - 1];
		n--;
		char *kptr = kk + 1;
		sprintf(kptr, "%08d", k);
		rv = tapioca_bptree_search(th, tbpt_id, kk, 10, vv, &vsize);
		//if (rv !=1) printf("THR_ID %d: Failed to find %s!\n",s->thread_id,kk);
		tapioca_commit(th);
	}

	clock_gettime(CLOCK_MONOTONIC, &tmend);
	runtime = (tmend.tv_nsec - tms.tv_nsec);
	druntime = (tmend.tv_sec - tms.tv_sec) + (runtime / 1000000000.0);
	printf("THR_ID %d: completed in %.3f seconds, %.2f searches/s \n",
			s->thread_id, druntime, (double) (s->keys / druntime));

	/// Test index scanning

	//assert(verify_tapioca_bptree_order(th,1));
	//tapioca_commit(th);

	free(arr);
	tapioca_commit(th);
	tapioca_close(th);
	return NULL;

}
void *thr_tapioca_bptree_insert_test(void *data)
{
	int i, j, k, v, n, r, rv;
	struct timespec tms, tmend;
	double druntime;
	long runtime;
	int* arr;
	tapioca_handle *th;
	struct thread_config *s = (struct thread_config *) data;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;
	tapioca_bptree_id tbpt_id;
	if (s->thread_id == 0)
	{
		th = tapioca_open("127.0.0.1", 5555);
		tbpt_id = tapioca_bptree_initialize_bpt_session(th, s->seed,
				BPTREE_OPEN_OVERWRITE);
	}
	else
	{
		th = tapioca_open("127.0.0.1", 5555);
		tbpt_id = tapioca_bptree_initialize_bpt_session(th, s->seed,
				BPTREE_OPEN_CREATE_IF_NOT_EXISTS);
	}
	if (th == NULL)
	{
		printf("Failed to connect to tapioca\n");
		return NULL;
	}
	tapioca_bptree_set_num_fields(th,tbpt_id, 1);
	tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);

	printf("THR_ID %d connected to node id %d", s->thread_id,
			tapioca_node_id(th));
	arr = (int *) malloc(s->keys * sizeof(int));
	for (i = 0; i < s->keys; i++)
		arr[i] = i + s->start_key;
	n = s->keys;
	char kk[10] = "aaaaaaaaa";
	char vv[10] = "cccc";
	printf("THR_ID %d: Writing keys starting from %d...\n", s->thread_id,
			s->start_key);
	clock_gettime(CLOCK_MONOTONIC, &tms);
	int total_retries = 0;
	for (i = 0; i < s->keys; i++)
	{
		r = rand() % (n);
		k = arr[r];
		arr[r] = arr[n - 1];
		n--;
		char *kptr = kk + 1;
		sprintf(kptr, "%08d", k);
		rv = -1;
		int attempts = 1;
		do
		{
			printf("Inserting k/v %s / %s \n", kk, vv);
			rv = tapioca_bptree_insert(th, tbpt_id, kk, 10, vv, 10,
					BPTREE_INSERT_UNIQUE_KEY);
			//if (rv != tapioca_bptree_ERR_DUPLICATE_KEY_INSERTED)
			// rv here could be DUPLICATE_KEY; do we care?
			if (rv < 0) {
				printf("Error on insert; will wait a bit\n");
				long wait = 5 * 100 * 1000 + (rand() % 100) * 1000;
				attempts++;
				usleep(wait);
			}
			else if (rv != BPTREE_ERR_DUPLICATE_KEY_INSERTED)
			{
				rv = tapioca_commit(th);
			}
			else
			{
				long wait = 100 * 1000 + (rand() % 100) * 1000;
				attempts++;
				usleep(wait);
			}

		} while (rv != 0 && attempts < 10);
		if (rv != 0)
		{
			printf("Could  not commit %s after %d tries\n", kk, attempts);
		}
		total_retries += attempts;

		tapioca_commit(th);
	}
	clock_gettime(CLOCK_MONOTONIC, &tmend);
	runtime = (tmend.tv_nsec - tms.tv_nsec);
	druntime = (tmend.tv_sec - tms.tv_sec) + (runtime / 1000000000.0);
	printf("THR_ID %d: completed in %.3f seconds, %.2f ins/s %d total tries \n",
			s->thread_id, druntime, (double) (s->keys / druntime),
			total_retries);

	free(arr);
	tapioca_commit(th);
	tapioca_close(th);
	return NULL;

}

void *thr_tapioca_bptree_serialization_test(void *data) // so we can instantiate as a thread
{
	int i, j, k, v, n, r, rv;
	struct timespec tms, tmend;
	double druntime;
	long runtime;
	int* arr;
	tapioca_handle *th;
	struct thread_config *s = (struct thread_config *) data;
	static char* address = (char *) "127.0.0.1";
	static int port = 5555;

	if (s->thread_id == 0)
	{
		th = tapioca_open("127.0.0.1", 5555);
		tapioca_bptree_initialize_bpt_session(th, s->seed, BPTREE_OPEN_OVERWRITE);
	}
	else
	{
		th = tapioca_open("127.0.0.1", 5555);
		tapioca_bptree_initialize_bpt_session(th, s->seed,
				BPTREE_OPEN_CREATE_IF_NOT_EXISTS);
	}
	if (th == NULL)
	{
		printf("Failed to connect to tapioca\n");
		return NULL;
	}
	return NULL;

}

void *thr_tapioca_bptree_test_concurrent_access(void *data)
{ // so that we can instantiate as a thread
	int rv, i, j, k, v;
	struct thread_config *s = (struct thread_config *) data;
	tapioca_handle *th;
	th = tapioca_open("127.0.0.1", 5555);
	k = 1;
	v = s->thread_id;
	printf("THR_ID %d start \n", s->thread_id);
	fflush(stdout);
	if (!s->thread_id == 0)
		sleep(5);

	rv = tapioca_put(th, &k, sizeof(int), &v, sizeof(int));
	rv = tapioca_get(th, &k, sizeof(int), &v, sizeof(int));

	printf("THR_ID %d pre-commit k/v:  %d / %d \n", s->thread_id, k, v);
	fflush(stdout);
	if (s->thread_id == 0)
		sleep(10);
	rv = tapioca_commit(th);
	printf("THR_ID %d commit rv:  %d\n", s->thread_id, rv);
	fflush(stdout);

	rv = tapioca_get(th, &k, sizeof(int), &v, sizeof(int));
	printf("THR_ID %d post-commit k/v:  %d / %d \n", s->thread_id, k, v);
	fflush(stdout);
	rv = tapioca_commit(th);
	return NULL;
}

TEST_F (BptreeConcurrencyTest, TestInsertAndTraverse) 
{
	//int rv, i, k, dbug, v, seed, num_threads;
	// Pre-insert
	for (int i = 0; i < num_threads; i++)
	{
		s[i].start_key = i * keys;
		s[i].seed = seed;
		s[i].dbug = dbug;
		s[i].keys = keys;
		s[i].thread_id = i;
		rv = pthread_create(&(threads[i]), NULL, thr_tapioca_bptree_insert_test, &s[i]);
		if (i == 0)
			usleep(500 * 1000); // Let the first thd destroy the existing b+tree
		usleep(50 * 1000); // Let the first thread destroy the existing b+tree
	}

	for (int i = 0; i < num_threads; i++)
	{
		rv = pthread_join(threads[i], NULL);
	}

	free(threads);
	threads = (pthread_t *) malloc(sizeof(pthread_t) * num_threads * 2);

	// Do concurrent traversal for starting keys, then insert more keys after
	// in parallel
	printf("Launching parallel insert/search\n");
	for (int i = 0; i < num_threads; i++)
	{
		rv = pthread_create(&(threads[i]), NULL, thr_tapioca_bptree_traversal_test, &s[i]);
		usleep(50 * 1000); // Let the first thread destroy the existing b+tree
	}

	for (int i = num_threads; i < num_threads * 2; i++)
	{
		s[i].start_key = i * keys;
		s[i].seed = seed;
		s[i].dbug = dbug;
		s[i].keys = keys;
		s[i].thread_id = i;
		rv = pthread_create(&(threads[i]), NULL, thr_tapioca_bptree_insert_test, &s[i]);
		usleep(50 * 1000); // Let the first thread destroy the existing b+tree
	}

	for (int i = 0; i < num_threads * 2; i++)
	{
		rv = pthread_join(threads[i], NULL);
	}

	if (dbug)
	{
		printf("====================================================\n");
		printf("Waiting a bit to dump b+tree contents:\n\n");
		usleep(1000 * 1000);
		tapioca_handle *th;
		th = tapioca_open("127.0.0.1", 5555);
		tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, s->seed,
				BPTREE_OPEN_CREATE_IF_NOT_EXISTS);
		tapioca_bptree_set_num_fields(th,tbpt_id, 1);
		tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);
		//		dump_tapioca_bptree_contents(th,tbpt_id,dbug,0);
	}
	free(threads);
	//return 1;
}

TEST_F(BptreeConcurrencyTest, TestInsertAndSearch)
{
	pthread_t *ins_threads;
	pthread_t *srch_threads;
	struct thread_config *ins;
	struct thread_config *srch;

	ins_threads = (pthread_t *) malloc(sizeof(pthread_t) * num_threads);
	srch_threads = (pthread_t *) malloc(sizeof(pthread_t) * num_threads);

	ins = (struct thread_config *) malloc(sizeof(struct thread_config) * num_threads);
	srch = (struct thread_config *) malloc(sizeof(struct thread_config) * num_threads);

	clock_gettime(CLOCK_MONOTONIC, &tms);
	for (int i = 0; i < num_threads; i++)
	{
		ins[i].start_key = i * keys;
		ins[i].seed = srch[i].seed = seed;
		ins[i].dbug = srch[i].dbug = dbug;
		ins[i].keys = srch[i].keys = keys;
		ins[i].thread_id = i;
		srch[i].thread_id = num_threads + i;
		srch[i].start_key = (num_threads - i) * keys;
		rv = pthread_create(&(ins_threads[i]), NULL, thr_tapioca_bptree_insert_test,
				&ins[i]);
		if (i == 0)
			usleep(500 * 1000); // Let the first thd destroy the existing b+tree
		// we can share the s struct cause nothing gets changed
		rv = pthread_create(&(srch_threads[i]), NULL, thr_tapioca_bptree_search_test,
				&srch[i]);
		usleep(50 * 1000); // Let the first thread destroy the existing b+tree
	}

	for (int i = 0; i < num_threads; i++)
	{
		rv = pthread_join(srch_threads[i], NULL);
		rv = pthread_join(ins_threads[i], NULL);
	}

	clock_gettime(CLOCK_MONOTONIC, &tmend);
	runtime = (tmend.tv_nsec - tms.tv_nsec);
	druntime = (tmend.tv_sec - tms.tv_sec) + (runtime / 1000000000.0);
	printf("*****\nInserts completed in %.3f seconds, %.2f ins/s \n\n",
			druntime, ((keys * num_threads) / druntime));

	free(ins_threads);
	free(srch_threads);

	if (dbug)
	{
		printf("====================================================\n");
		printf("Waiting a bit to dump b+tree contents:\n\n");
		usleep(1000 * 1000);
		tapioca_handle *th;
		th = tapioca_open("127.0.0.1", 5555);
		tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, seed,
				BPTREE_OPEN_ONLY);
		tapioca_bptree_set_num_fields(th,tbpt_id, 1);
		tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);
		//		dump_tapioca_bptree_contents(th, tbpt_id,dbug,0);
	}
	//return 1;
}

TEST_F(BptreeConcurrencyTest, TestInsertThenSearch)
{
	clock_gettime(CLOCK_MONOTONIC, &tms);
	for (int i = 0; i < num_threads; i++)
	{
		s[i].start_key = i * keys;
		s[i].seed = seed;
		s[i].dbug = dbug;
		s[i].keys = keys;
		s[i].thread_id = i;
		rv = pthread_create(&(threads[i]), NULL, thr_tapioca_bptree_insert_test, &s[i]);
		if (i == 0)
			usleep(500 * 1000); // Let first thread destroy the existing b+tree
		usleep(50 * 1000); // Let the first thread destroy the existing b+tree
	}

	for (int i = 0; i < num_threads; i++)
	{
		rv = pthread_join(threads[i], NULL);
	}

	clock_gettime(CLOCK_MONOTONIC, &tmend);
	runtime = (tmend.tv_nsec - tms.tv_nsec);
	druntime = (tmend.tv_sec - tms.tv_sec) + (runtime / 1000000000.0);
	printf("*****\nInserts completed in %.3f seconds, %.2f ins/s \n\n",
			druntime, ((keys * num_threads) / druntime));

	free(threads);
	threads = (pthread_t *) malloc(sizeof(pthread_t) * num_threads);

	clock_gettime(CLOCK_MONOTONIC, &tms);
	for (int i = 0; i < num_threads; i++)
	{
		rv = pthread_create(&(threads[i]), NULL, thr_tapioca_bptree_search_test, &s[i]);
		usleep(50 * 1000); // Let the first thread destroy the existing b+tree
	}

	for (int i = 0; i < num_threads; i++)
	{
		rv = pthread_join(threads[i], NULL);
	}

	clock_gettime(CLOCK_MONOTONIC, &tmend);
	runtime = (tmend.tv_nsec - tms.tv_nsec);
	druntime = (tmend.tv_sec - tms.tv_sec) + (runtime / 1000000000.0);
	printf("*****\nSearches completed in %.3f seconds, %.2f searches/s \n",
			druntime, ((keys * num_threads) / druntime));

	if (dbug)
	{
		printf("====================================================\n");
		printf("Waiting a bit to dump b+tree contents:\n\n");
		usleep(1000 * 1000);
		tapioca_handle *th;
		th = tapioca_open("127.0.0.1", 5555);
		tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, s->seed,
				BPTREE_OPEN_CREATE_IF_NOT_EXISTS);
		tapioca_bptree_set_num_fields(th,tbpt_id, 1);
		tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);
		//		dump_tapioca_bptree_contents(th, tbpt_id,dbug,0);
	}
	free(threads);
	//return 1;
}