Tapioca Storage System
======================

MoSQL depends on the "Tapioca" storage system which must be built and deployed along with the MySQL server and MoSQL storage engine. 

Building
--------

Create a build directory with CMake, and run CMake to create the makefiles.


    mkdir Debug
    cd Debug
    cmake -D LIBPAXOS_ROOT=/home/atomic/branches/libpaxos_uc/Debug -D CMAKE_BUILD_TYPE=Debug ../src

Then run

    make -j 2

You will need to have libevent 2.0, msgpack, Berkeley DB 4.8 or greater and libuuid. 

Usage
-----

The interactive shell allows you to open a connection to a Tapioca node
and issue transactions against it. The following example shows how to put
and get a key:

./bin/tshell
enter `?' for help.
> open
tapioca@0 > put 1 1
committed
tapioca@0 > get 1
1
committed

The open command connects by default to ip 127.0.0.1 on port 5555. Ip and
port can be given as optional parameters. Also, by default get and put 
operations are committed automatically by the shell. You can issue 
transactions with multiple operations using begin and commit commands:

tapioca@0 > begin
tapioca@0 > put 1 1
tapioca@0 > put 2 2
tapioca@0 > put 3 3
tapioca@0 > get 3
3
committed
tapioca@0 > commit
committed

The shell distinguishes two different data types: integers and strings.
As opposed to integers, a string is delimited by quotes. In addition to the
get command, the shell supports the gets command which interprets the
value returned as a string. The following example demonstrates how to use 
strings in the shell:

tapioca@0 > put "1" "value"
committed
tapioca@0 > gets "1"
"value"
committed
tapioca@0 > get 1
null
	
C API
-----
	
In order to use Tapioca's API you need to #include <tapioca.h> and link
against libtapioca.a. Both the header file and the static library are
located in libtapioca/.

Connecting to a Tapioca node:

    #include <tapioca.h>
    
    static int port = 8080;
    static char* address = "127.0.0.1";
    
    int main(int argc, char *argv[]) {
    	tapioca_handle* th;
    	
    	th = tapioca_open(address, port);
    	if (th == NULL) {
    		fprintf(stderr, "Failed to open tapioca")
    		return 1;
    	}
    	...
    	tapioca_close(th);
    	return 0;
    }

Implementing a transaction that stores an integer key value pair:

    static int tapioca_put_int(tapioca_handle* th, int k, int v)  {
    	int rv;
     	rv = tapioca_put(th, &k, sizeof(int), &v, sizeof(int));
    	if (rv == -1) return -1;
    	return tapioca_commit(th);
    }

Implementing a transaction that retrieve an integer key:

    static int tapioca_get_int(tapioca_handle* th, int k, int* v) {
    	int rv;
    	rv = tapioca_get(th, &k, sizeof(int), v, sizeof(int));
    	if (rv == -1) return -1;
    	return tapioca_commit(th);
    }

See test/example.c for the complete example.
