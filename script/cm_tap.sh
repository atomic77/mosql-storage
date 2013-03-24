#!/bin/bash

killall -q -INT cm tapioca
sleep 0.5
killall -q -9 cm tapioca

cd ~/branches/tapioca_bdb/Release

# Launch tapioca node and CM

bin/cm config/1.cfg config/config.cfg &    
sleep 0.5
bin/tapioca 0 config/1.cfg config/config.cfg 5555 &

cd -
