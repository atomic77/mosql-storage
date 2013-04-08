#!/bin/bash

killall -q -INT cm tapioca
sleep 0.5
killall -q -9 cm tapioca

# Launch tapioca node and CM

bin/cm config/1.cfg config/paxos_config.cfg &    
sleep 0.5
bin/tapioca 0 config/1.cfg config/paxos_config.cfg 5555 &

