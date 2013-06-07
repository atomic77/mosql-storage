MoSQL Storage System
======================

The MoSQL storage layer (mosql-storage) is a single component of the larger MoSQL system developed at the Unviersity of Lugano, now released under the GPL v3. For an overview, please see the [main project home page](http://dslab.inf.usi.ch/mosql/).

The MoSQL storage system for MySQL/MariaDB requires [libpaxos] in order to function. The MoSQL storage layer is a fully-functioning transactional key-value system on its own, useful for applications which do not require full-fledged SQL transactions as provided by the full MoSQL system. If you are looking for the MoSQL storage engine you can find it at [mosql-se]. 

Dependencies
------------

To build, you will require:

* [libpaxos] 
* BerkeleyDB 4.8 or higher (eg. libdb5.1-dev on ubuntu, libdb-devel on Fedora/RH)
* libevent 2.0 (libevent-dev on debian/ubuntu, libevent-devel on Fedora/RH)
* libUUID (uuid-dev on debian/ubuntu, libuuid-devel on fedora)
* msgpack (libmsgpack-dev on debian/ubuntu, msgpack-devel? on fedora)
* Google Profiling tools (gperftools on fedora) -- this is not really required but is currently linked in on Debug builds for testing

It should be possible to get everything working with OSX and possibly even Cygwin although the majority of our testing happens on Linux platforms.

Building
--------

The MoSQL storage layer uses CMake. To build, enter the root folder of mosql-storage that you cloned from our public git repository and run:

    mkdir build
    cd build
    cmake -D LIBPAXOS_ROOT=/home/user/local/libpaxos -D CMAKE_INSTALL_PREFIX=/home/user/local/mosql-storage -D CMAKE_BUILD_TYPE=<buildType> ../src
    
Since it is likely you are building libpaxos from source, you can use the LIBPAXOS_ROOT parameter to CMake to tell the build process where to find installation folder. The CMake modules provided should find BDB, libuuid, msgpack and libevent for you if they are installed, but if needed you can override the base directories for these as well (BDB_ROOT, LIBUUID_ROOT, MSGPACK_ROOT, LIBEVENT_ROOT). <buildType> should be one of Debug, Release, RelWithDbgInfo. You can also use the CMAKE_INSTALL_PREFIX parameter to tell the build process where it should install the mosql-storage binaries and configuration files. 

Installation
------------

Assuming CMake ran successfully, run:

    make -j 4
    make install
    
To build and install mosql-storage into the target installation folder. 


Launching
---------

In the install folder, use the script/launch_all.sh script to easily launch all of the required processes on a single host for testing purposes. Run launch_all.sh --help for a list of options.

Usage
-----

With a running storage layer, the interactive shell allows you to open a connection to a node
and issue transactions against it. This can be useful in cases where you want to be sure the storage layer is working and can commit trasnactions, even if you are only using the SQL interface. The following example shows how to put and get a key:

    bin/tshell
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

The shell distinguishes two different data types: integers and strings. As opposed to integers, a string is delimited by quotes. In addition to the get command, the shell supports the gets command which interprets the value returned as a string. The following example demonstrates how to use strings in the shell:

    tapioca@0 > put "1" "value"
    committed
    tapioca@0 > gets "1"
    "value"
    committed
    tapioca@0 > get 1
    null
	
C API
-----
	
In order to use the storage layer API you need to #include <tapioca/tapioca.h> and link against libtapioca.a. To use the indexing functions you must also include <tapioca/tapioca_btree.h> header files. The header files each contain documentation for the given methods. 

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


See src/test/example.c for an example of a program that uses the C API. A native Java library does exist although is unmaintained -- if it is useful to someone we can make it available.

[mosql-se]: https://bitbucket.org/atomic77/mosql-se
[mosql-storage]: https://bitbucket.org/atomic77/mosql-storage
[libpaxos]: https://bitbucket.org/sciascid/libpaxos


