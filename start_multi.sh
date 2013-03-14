#!/bin/bash

NODES=$1
if [ $# -ne 1 ]; then
	echo "Usage: $0 <NumNodes>"
	echo "For the number of nodes given, there must be a file in config <NumNodes>.cfg"
	echo "Ports will be assigned from 5555 up."
	exit 1
fi
tapioca="./bin/tapioca"
cm="./bin/cm"
acceptor="./bin/acceptor"

paxos_config="config/paxos.cfg"
tapioca_config="config/$1.cfg"

source "script/launchers.sh"

run_acceptors
run_cm $tapioca_config
for n in `seq 0 $(( $NODES - 1 ))`; do
	sleep 1
    PORT=$(( 5555 + $n ))
	run_tapioca $n $tapioca_config $PORT
done
./bin/rec 2 $paxos_config $tapioca_config > rec_2.log &
./bin/rec 3 $paxos_config $tapioca_config 12346 > rec_3.log &
#valgrind --tool=exp-dhat  ./bin/rec 3 $paxos_config $tapioca_config 12346 > rec_3.log 2> dhat.out
#valgrind --leak-check=full  ./bin/rec 3 $paxos_config $tapioca_config 12346 > rec_3.log  &

