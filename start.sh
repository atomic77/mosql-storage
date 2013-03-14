#!/bin/bash

tapioca="./bin/tapioca"
cm="./bin/cm"
acceptor="./bin/acceptor"

paxos_config="config/config.cfg"
tapioca_config="config/1.cfg"

source "script/launchers.sh"

run_acceptors
run_cm $tapioca_config
run_tapioca 0 $tapioca_config 5555
./bin/rec 2 $paxos_config $tapioca_config > rec_2.log &
./bin/rec 3 $paxos_config $tapioca_config 12346 > rec_3.log &
#valgrind --tool=exp-dhat  ./bin/rec 3 $paxos_config $tapioca_config 12346 > rec_3.log 2> dhat.out
#valgrind --leak-check=full  ./bin/rec 3 $paxos_config $tapioca_config 12346 > rec_3.log  &

