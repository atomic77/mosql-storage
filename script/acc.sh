#!/bin/bash 

PAXOS_DIR=/home/atomic/local/libpaxos

if [ ! -e config/paxos_config.cfg ]; then
  echo "Please run this script from the root install folder"
  exit 1
fi

if [ ! -e $PAXOS_DIR/bin/example_acceptor ]; then
  echo "Provide a correct libpaxos directory, did not find acceptor"
  echo "in $PAXOS_DIR/bin"
  exit 1
fi

# Launch acceptors and proposer
echo "Launching acceptors"

killall -q -INT example_proposer example_acceptor rec
sleep 0.5
killall -q -9 example_proposer example_acceptor rec

#cd $PAXOS_DIR
for i in 2 1 0; do
	$PAXOS_DIR/bin/example_acceptor $i config/paxos_config.cfg &
	sleep 0.4
done

sleep 1

$PAXOS_DIR/bin/example_proposer 0 config/paxos_config.cfg &

echo "Launching rec nodes"

#for i in 2 1 0; do
	bin/rec 0 config/paxos_config.cfg config/1.cfg &
#done

wait

