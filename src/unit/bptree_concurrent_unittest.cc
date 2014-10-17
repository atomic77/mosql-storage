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
#include <vector>

using namespace std;

struct thread_config
{
	tapioca_bptree_id seed;
	int dbug;
	int keys;
	int thread_id;
	int start_key;
	const char *hostname;
	int port;
	bool strict;
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
	
	//static void *thr_tapioca_bptree_traversal_test(void *data);
	
	void SetUp() {
		BptreeTestBase::SetUp();
		num_threads = 8;
		keys = 500;
		DBUG = false;
		dbug = 0; 
		seed = tbpt_id;
		srand(seed);

		threads = (pthread_t *) malloc(sizeof(pthread_t) * num_threads);

		s = (struct thread_config *) 
			malloc(sizeof(struct thread_config) * num_threads * 2);

	}

	
	vector<string> retrieveKeysViaScan(tapioca_handle *t, tapioca_bptree_id t_id) {
		vector<string> keyvect(0);
		int rv;
		rv = tapioca_bptree_index_first_no_key(t, t_id);
		for (;;) {
			rv = tapioca_bptree_index_next(t, t_id, &k, &ksize, &v, &vsize);
			if (rv != BPTREE_OP_KEY_FOUND) break;
			string kk(k);
			keyvect.push_back(kk);
		}
		return keyvect;
	}
	vector<string> retrieveValuesViaScan(tapioca_handle *t, tapioca_bptree_id t_id) {
		vector<string> valuevect(0);
		int rv;
		rv = tapioca_bptree_index_first_no_key(t, t_id);
		for (;;) {
			rv = tapioca_bptree_index_next(t, t_id, &k, &ksize, &v, &vsize);
			if (rv != BPTREE_OP_KEY_FOUND) break;
			string vv(v);
			valuevect.push_back(vv);
		}
		return valuevect;
	}
	bool verifyKeys(vector<string> keyvect, int start, int end) {
		
		char kk[10] = "aaaaaaaaa";
		int idx = 0;
		for (int i = start; i < end; i++ ) {
			sprintf(kk, "a%08d", i);
			string st = keyvect[idx];
			if (st.compare(kk) != 0) return false;
			idx++;
		}
		
		return true;
	}
};

class BptreeMultiNodeTest : public BptreeConcurrencyTest {
protected:
	// add multi-node specific stuff here
	void addNode(int type, int id) {
		char tcmd[256];
		printf("Launching node type %d...\n", type);
		system ("screen -S tcache -X quit &> /dev/null ; echo 'run' > /tmp/_cmd ");
                sprintf(tcmd, "cd ..; screen -d -m -S tnode_t%d_n%d gdb -x /tmp/_cmd --args bin/tapioca --ip-address 127.0.0.1 --port %d --storage-config config/1.cfg --paxos-config config/paxos_config.cfg --node-type %d ", type, id, 5555+id, type);
                system(tcmd);
		sleep(2);
	}
	
	void addRegularNode(int id) {
		addNode(0, id);
	}
	void addCacheNode(int id) {
		addNode(1, id);
	}
};

void* thr_tapioca_bptree_traversal_test(void* data)
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

	th = tapioca_open(s->hostname, s->port);
	tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, s->seed,
			BPTREE_OPEN_CREATE_IF_NOT_EXISTS, BPTREE_INSERT_UNIQUE_KEY);
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

	printf("Traversal Thread %d: Traversing %d keys starting from a%08d...\n", 
		   s->thread_id, s->keys, s->start_key);
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
		sprintf(kk, "a%08d", k);
		rv = tapioca_bptree_search(th, tbpt_id, kk, 10, vv, &vsize);
		EXPECT_EQ(rv, BPTREE_OP_KEY_FOUND);
		int cnt = 0;
		do
		{
			rv = tapioca_bptree_index_next(th, tbpt_id, k2, &ksize, v2, &vsize);
			if (rv != BPTREE_OP_EOF) EXPECT_GE(memcmp(k2, kk, 10), 0);
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

	th = tapioca_open(s->hostname, s->port);
	tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, s->seed,
			BPTREE_OPEN_CREATE_IF_NOT_EXISTS, BPTREE_INSERT_UNIQUE_KEY);
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

	printf("Search Thread %d: Searching for %d keys starting from a%08d...\n", 
		   s->thread_id, s->keys, s->start_key);
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
		//printf("Searching for k %s  \n", kk);
		rv = tapioca_bptree_search(th, tbpt_id, kk, 10, vv, &vsize);
		if(s->strict) EXPECT_TRUE(rv == BPTREE_OP_KEY_FOUND);
		EXPECT_TRUE(rv == BPTREE_OP_KEY_FOUND | rv == BPTREE_OP_KEY_NOT_FOUND);
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
	tapioca_bptree_id tbpt_id;
	if (s->thread_id == 0)
	{
		th = tapioca_open(s->hostname, s->port);
		tbpt_id = tapioca_bptree_initialize_bpt_session(th, s->seed,
				BPTREE_OPEN_OVERWRITE, BPTREE_INSERT_UNIQUE_KEY);
		EXPECT_EQ(tbpt_id, s->seed);
	}
	else
	{
		th = tapioca_open(s->hostname, s->port);
		tbpt_id = tapioca_bptree_initialize_bpt_session(th, s->seed,
				BPTREE_OPEN_CREATE_IF_NOT_EXISTS, BPTREE_INSERT_UNIQUE_KEY);
		EXPECT_EQ(tbpt_id, s->seed);
	}
	if (th == NULL)
	{
		printf("Failed to connect to tapioca\n");
		return NULL;
	}
	tapioca_bptree_set_num_fields(th,tbpt_id, 1);
	tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);

	//printf("THR_ID %d connected to node id %d", s->thread_id,
			//tapioca_node_id(th));
	arr = (int *) malloc(s->keys * sizeof(int));
	for (i = 0; i < s->keys; i++)
		arr[i] = i + s->start_key;
	n = s->keys;
	char kk[10] = "aaaaaaaaa";
	char vv[10] = "cccc";
	printf("Insert Thread %d: Writing %d keys starting from a%08d...\n", 
		   s->thread_id, s->keys, s->start_key);
	clock_gettime(CLOCK_MONOTONIC, &tms);
	int total_retries = 0;
	for (i = 0; i < s->keys; i++)
	{
		r = rand() % (n);
		k = arr[r];
		arr[r] = arr[n - 1];
		n--;
		sprintf(kk, "a%08d", k);
		rv = -1;
		int attempts = 1;
		do
		{
			//printf("Inserting k/v %s / %s \n", kk, vv);
			rv = tapioca_bptree_insert(th, tbpt_id, kk, 10, vv, 10);
			EXPECT_GE(rv, BPTREE_OP_SUCCESS);
			rv = tapioca_commit(th);
			if (rv < 0)
			{
				long wait = 100 * 1000 + (rand() % 100) * 1000;
				attempts++;
				usleep(wait);
			}

		} while (rv < 0 && attempts < 10);
		EXPECT_GE(rv, 0);
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
		s[i].hostname = hostname;
		s[i].port = port;
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
		s[i].hostname = hostname;
		s[i].port = port;
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
				BPTREE_OPEN_CREATE_IF_NOT_EXISTS, BPTREE_INSERT_UNIQUE_KEY);
		tapioca_bptree_set_num_fields(th,tbpt_id, 1);
		tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);
		//		dump_tapioca_bptree_contents(th,tbpt_id,dbug,0);
	}
	free(threads);
	
	th = tapioca_open("127.0.0.1", 5555);
	tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, s->seed,
			BPTREE_OPEN_CREATE_IF_NOT_EXISTS, BPTREE_INSERT_UNIQUE_KEY);
	tapioca_bptree_set_num_fields(th,tbpt_id, 1);
	tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);
	vector<string> kvec = retrieveKeysViaScan(th, tbpt_id);
	EXPECT_EQ(kvec.size(), num_threads * keys);
	vector<string> vvec = retrieveValuesViaScan(th, tbpt_id);
	EXPECT_EQ(vvec.size(), num_threads * keys);
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
		ins[i].hostname = hostname;
		ins[i].port = port;
		srch[i].thread_id = num_threads + i;
		srch[i].start_key = i * keys;
		srch[i].hostname = hostname;
		srch[i].port = port;
		srch[i].strict = false;
		rv = pthread_create(&(ins_threads[i]), NULL, thr_tapioca_bptree_insert_test,
				&ins[i]);
		if (i == 0)
			usleep(500 * 1000); // Let the first thd destroy the existing b+tree
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
				BPTREE_OPEN_ONLY, BPTREE_INSERT_UNIQUE_KEY);
		tapioca_bptree_set_num_fields(th,tbpt_id, 1);
		tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);
		//		dump_tapioca_bptree_contents(th, tbpt_id,dbug,0);
	}
	th = tapioca_open("127.0.0.1", 5555);
	tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, s->seed,
			BPTREE_OPEN_CREATE_IF_NOT_EXISTS, BPTREE_INSERT_UNIQUE_KEY);
	tapioca_bptree_set_num_fields(th,tbpt_id, 1);
	tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);
	vector<string> kvec = retrieveKeysViaScan(th, tbpt_id);
	EXPECT_EQ(kvec.size(), num_threads * keys);
	vector<string> vvec = retrieveValuesViaScan(th, tbpt_id);
	EXPECT_EQ(vvec.size(), num_threads * keys);
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
		s[i].hostname = hostname;
		s[i].port = port;
		s[i].strict = true;
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
				BPTREE_OPEN_CREATE_IF_NOT_EXISTS, BPTREE_INSERT_UNIQUE_KEY);
		tapioca_bptree_set_num_fields(th,tbpt_id, 1);
		tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);
		//		dump_tapioca_bptree_contents(th, tbpt_id,dbug,0);
	}
	free(threads);
	
	th = tapioca_open("127.0.0.1", 5555);
	tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, s->seed,
			BPTREE_OPEN_CREATE_IF_NOT_EXISTS, BPTREE_INSERT_UNIQUE_KEY);
	tapioca_bptree_set_num_fields(th,tbpt_id, 1);
	tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);
	vector<string> kvec = retrieveKeysViaScan(th, tbpt_id);
	EXPECT_EQ(kvec.size(), num_threads * keys);
	vector<string> vvec = retrieveValuesViaScan(th, tbpt_id);
	EXPECT_EQ(vvec.size(), num_threads * keys);
	
	//return 1;
}

TEST_F(BptreeMultiNodeTest, TestSimpleCacheNodeAddPutGet)
{
	//int k, v, rv, ksz, vsz;
	int rv, ksz, vsz;
	char k[5], v[5], v2[5];
	sprintf(k, "k%03d", 1);
	sprintf(v, "v%03d", 1);
	memset(v2, 0, 5);
	rv = tapioca_put(th, k, strlen(k), v, strlen(v));
	rv = tapioca_get(th, k, strlen(k), v2, 5);
	EXPECT_TRUE(strncmp(v, v2, 5) == 0);
	rv = tapioca_commit(th);
	EXPECT_GE(0, rv);
	
	addCacheNode(1);
	
	tapioca_handle *th_cache = tapioca_open(hostname, 5556);
	ASSERT_TRUE(th_cache != NULL);
	memset(v2, 0, 5);
	rv = tapioca_get(th_cache, k, strlen(k), v2, 5);
	EXPECT_TRUE(strncmp(v, v2, 5) == 0);
	
	
}
TEST_F(BptreeMultiNodeTest, TestSimpleCacheNodeAddBt)
{
	int k, v, rv, ksz, vsz;
	k = v = 1234;
	tapioca_bptree_set_num_fields(th, tbpt_id, 1);
	tapioca_bptree_set_field_info(th, tbpt_id, 0, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	rv = tapioca_bptree_insert(th, tbpt_id, &k, sizeof(int), &v, sizeof(int));
	EXPECT_EQ(BPTREE_OP_SUCCESS, rv);
	rv = tapioca_commit(th);
	EXPECT_GE(0, rv);
	
	addCacheNode(1);
	
	// Create new node
	tapioca_handle *th_cache = tapioca_open(hostname, 5556);
	ASSERT_TRUE(th_cache != NULL);
	rv = tapioca_bptree_initialize_bpt_session_no_commit(th_cache, tbpt_id, BPTREE_OPEN_ONLY, 
					      BPTREE_INSERT_UNIQUE_KEY, 2);
	tapioca_bptree_set_num_fields(th_cache, tbpt_id, 1);
	tapioca_bptree_set_field_info(th_cache, tbpt_id, 0, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	
	v = 0;
	rv = tapioca_bptree_search(th_cache, tbpt_id, &k, sizeof(int), &v, &vsz);
	EXPECT_EQ(v, 1234);
	
	
}

TEST_F(BptreeMultiNodeTest, TestSimpleMultiCacheNodes)
{
	int k, v, rv, ksz, vsz;
	k = v = 1234;
	
	int nodes = 4;
	num_threads = nodes * 3;
	keys = 50;
	DBUG = true;
	
	tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, seed,
			BPTREE_OPEN_ONLY, BPTREE_INSERT_UNIQUE_KEY);
	tapioca_bptree_set_num_fields(th,tbpt_id, 1);
	tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);
	rv = tapioca_commit(th);
	EXPECT_GE(0, rv);
	
	tapioca_handle **thandles = (tapioca_handle **)
		malloc(sizeof(tapioca_handle*) * nodes);
	threads = (pthread_t *) malloc(sizeof(pthread_t) * num_threads);
	for (int i = 0; i< nodes; i++) {
		addCacheNode(i+1);
		thandles[i] = tapioca_open(hostname, 5556+i);
		ASSERT_TRUE(thandles[i] != NULL);
		//rv = tapioca_bptree_initialize_bpt_session_no_commit(thandles[i], tbpt_id, BPTREE_OPEN_ONLY, 
			//			BPTREE_INSERT_UNIQUE_KEY, 2);
		//tapioca_bptree_set_num_fields(thandles[i], tbpt_id, 1);
		//tapioca_bptree_set_field_info(thandles[i], tbpt_id, 0, sizeof(int32_t), BPTREE_FIELD_COMP_INT_32);
	}
	
	
	clock_gettime(CLOCK_MONOTONIC, &tms);
	for (int i = 0; i < num_threads; i++)
	{
		s[i].start_key = i * keys;
		s[i].seed = seed;
		s[i].dbug = dbug;
		s[i].keys = keys;
		s[i].thread_id = i;
		s[i].hostname = hostname;
		s[i].port = 5556 + (i % nodes);
		printf("Insert thread %d connecting to port %d\n", i, s[i].port);
		s[i].strict = true;
		rv = pthread_create(&(threads[i]), NULL, thr_tapioca_bptree_insert_test, &s[i]);
		if (i == 0)
			usleep(500 * 1000); // Let first thread destroy the existing b+tree
		//usleep(1 * 1000 * 1000);
		usleep(50 * 1000); // Let the first thread destroy the existing b+tree
	}

	for (int i = 0; i < num_threads; i++)
	{
		rv = pthread_join(threads[i], NULL);
	}

	printf("Inserts finished\n");
	
	free(threads);
	threads = (pthread_t *) malloc(sizeof(pthread_t) * num_threads);

	clock_gettime(CLOCK_MONOTONIC, &tms);
	for (int i = 0; i < num_threads; i++)
	{
		// connect to a random node rather than what we inserted to
		s[i].port = 5556 + (rand() % nodes);
		printf("Search thread %d connecting to port %d\n", i, s[i].port);
		//s[i].port = 5555; // + (rand() % nodes);
		rv = pthread_create(&(threads[i]), NULL, thr_tapioca_bptree_search_test, &s[i]);
		usleep(50 * 1000); 
	}

	for (int i = 0; i < num_threads; i++)
	{
		rv = pthread_join(threads[i], NULL);
	}

	//printf("Press a key to finish...\n");
	//getc(stdin);
	
	vector<string> kvec = retrieveKeysViaScan(th, tbpt_id);
	EXPECT_EQ(kvec.size(), num_threads * keys);
	vector<string> vvec = retrieveValuesViaScan(th, tbpt_id);
	EXPECT_EQ(vvec.size(), num_threads * keys);
	
	v = 0;
	
}

TEST_F(BptreeMultiNodeTest, TestInterleavedCacheAddition)
{
	int k, v, rv, ksz, vsz;
	k = v = 1234;
	
	int nodes = 4;
	num_threads = nodes * 1;
	keys = 2000;
	DBUG = true;
	
	// Launch first memory-based node
	tapioca_bptree_id tbpt_id = tapioca_bptree_initialize_bpt_session(th, seed,
			BPTREE_OPEN_ONLY, BPTREE_INSERT_UNIQUE_KEY);
	tapioca_bptree_set_num_fields(th,tbpt_id, 1);
	tapioca_bptree_set_field_info(th,tbpt_id, 0, 10, BPTREE_FIELD_COMP_STRNCMP);
	rv = tapioca_commit(th);
	EXPECT_GE(0, rv);
	
	thread_config s; // base config
	s.start_key = 0;
	s.seed = seed;
	s.dbug = dbug;
	s.keys = keys;
	s.thread_id = 0;
	s.hostname = hostname;
	s.port = 5555;
	s.strict = true;
		
	pthread_t thdbase; // = (pthread_t *) malloc(sizeof(pthread_t));
	rv = pthread_create(&thdbase, NULL, thr_tapioca_bptree_insert_test, &s);
	
	pthread_t *threads = (pthread_t *) malloc(sizeof(pthread_t) * num_threads);
	for (int i = 1; i <= nodes; i++) {
		addCacheNode(i);
		tapioca_handle *th1 = NULL;
		int connects = 10;
		while (th1 == NULL && connects > 0)
		{
			usleep(500 * 1000);
			th1 = tapioca_open(hostname, 5556);
			connects--;
		}
		// Cache node is up and ready
		ASSERT_TRUE(th1 != NULL);
		pthread_t thd1 ; //= (pthread_t *) malloc(sizeof(pthread_t));
		thread_config * s1 = (thread_config *) malloc(sizeof(thread_config));
		memcpy(s1, &s, sizeof(s));
		s1->thread_id = i;
		s1->start_key = keys * i;
		s1->port = 5555 + i;
		rv = pthread_create(&(threads[i-1]), NULL, thr_tapioca_bptree_insert_test, s1);
	}
	
	
	
	rv = pthread_join(thdbase, NULL);
	for (int i = 0; i < nodes; i++) {
		rv = pthread_join(threads[i], NULL);
	}

	printf("Threads complete, verifying data on \n");
	//getc(stdin);
	
	vector<string> kvec = retrieveKeysViaScan(th, tbpt_id);
	EXPECT_EQ(kvec.size(), (num_threads + 1)* keys);
	bool res = verifyKeys(kvec, 0, (num_threads +1)*keys);
	EXPECT_TRUE(res);
	vector<string> vvec = retrieveValuesViaScan(th, tbpt_id);
	EXPECT_EQ(vvec.size(), (num_threads + 1) * keys);
	
	v = 0;
	
}