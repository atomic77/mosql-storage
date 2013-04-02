#!/bin/bash

if [ ! -e config/1.cfg ]; then
  echo "Please run this script from the root install folder"
  exit 1
fi

killall -q -INT cm tapioca
sleep 0.5
killall -q -9 cm tapioca

# Launch tapioca node and CM

bin/cm config/1.cfg config/paxos_config.cfg &    
sleep 0.5
bin/tapioca 0 config/1.cfg config/paxos_config.cfg 5555 &

