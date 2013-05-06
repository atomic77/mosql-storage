#include "gtest.h"
#include "tapioca.h"
#include "util.h"
#include "test_helpers.h"


static const char* ip = "127.0.0.1";
static const int port = 5555; 


static const int nkeys = 500;
static const int nthreads = 48;
static const int iterations = 1000;

static int total = 0;
static pthread_mutex_t total_mutex;


class ContentionTest : public testing::Test {

protected:
    
    tapioca_handle* th;
    
	virtual void SetUp() {
		//system("cd ..; ./start.sh > /dev/null; cd unit");
		system("cd ..; bash scripts/launch_all.sh --kill-all --clear-db > /dev/null; cd -");
		sleep(1);
        th = tapioca_open("127.0.0.1", 5555);
        EXPECT_NE(th, (tapioca_handle*)NULL);
	}
	
	virtual void TearDown() {
        tapioca_close(th);
		//system("cd ..; ./stop.sh; rm *.log; sleep 2; rm -rf /tmp/[pr]log_*; cd unit");
		system("killall -q cm tapioca example_acceptor example_proposer rec");
		sleep(2);
		system("killall -q -9 cm tapioca example_acceptor example_proposer rec");
	}

/*	virtual void SetUp() {
        int usec = 150000;
                
        usleep(usec);
		system("cd ..; ./start_multi.sh 2 > /dev/null; cd unit");
        usleep(usec);
        
        th = tapioca_open(ip, port);
        EXPECT_NE(th, (tapioca_handle*)NULL);
	}
	
	virtual void TearDown() {
        system("cd ..; ./stop.sh; rm *.log; sleep 2; rm -rf /tmp/[pr]log_*; cd unit");
	}
	*/
};


static int tapioca_get_int(tapioca_handle* th, int k, int* v) {
	return tapioca_get(th, &k, sizeof(int), v, sizeof(int));
}


static int tapioca_put_int(tapioca_handle* th, int k, int v) {
	return tapioca_put(th, &k, sizeof(int), &v, sizeof(int));
}


static void* thread(void* arg) {
	int rv;
	int k, v, r;
	int total_t = 0;
	tapioca_handle* th;
	int last_seen[nkeys];
	
	for (int i = 0; i < nkeys; i++) {
		last_seen[i] = -1;
	}
	
	srandom(time(NULL));

	th = tapioca_open(ip, port);
	if (th == NULL) {
		printf("tapioca_open() failed\n");
		return NULL;
	}
	
	for (int i = 0; i < iterations; i++) {
		r = random_between(0, 100);
		k = random_between(0, nkeys);
		
		rv = tapioca_get_int(th, k, &v);
		EXPECT_EQ(rv, (int)sizeof(int));
		
		tapioca_put_int(th, k, v+r);
		
		rv = tapioca_commit(th);
		
		if (rv >= 0) {
			last_seen[k] = v;
			total_t += r;
		}
	}
	
	pthread_mutex_lock(&total_mutex);
	total += total_t;
	pthread_mutex_unlock(&total_mutex);
	
	tapioca_close(th);
	return NULL;
}


static void start_threads(int thr) {
	int i, rv;
	pthread_t threads[thr];
	
	for (i = 0; i < thr; i++) {
		rv = pthread_create(&threads[i], NULL, thread, NULL);
		assert(rv == 0);
	}
	
	for (i = 0; i < thr; i++) {
		pthread_join(threads[i], NULL);
	}
}


TEST_F(ContentionTest, CounterTest) {	
	int rv, v, sum = 0;

	pthread_mutex_init(&total_mutex, NULL);

    for (int i = 0; i < nkeys; i++) {
        rv = tapioca_put_int(th, i, 0);
		if (i % 10 == 0) tapioca_commit(th);
    }
	tapioca_commit(th);

    start_threads(nthreads);

    for (int i = 0; i < nkeys; i++) {
        rv = tapioca_get_int(th, i, &v);
		EXPECT_EQ(rv, (int)sizeof(int));
		sum += v;
    }
	
	EXPECT_EQ(sum, total);
}
