
PAXOS_DIR=/home/atomic/branches/libpaxos/Release/

# Launch acceptors and proposer

killall -q -INT example_proposer example_acceptor
sleep 0.5
killall -q -9 example_proposer example_acceptor

cd $PAXOS_DIR
for i in 2 1 0; do
	./tests/example_acceptor $i ../config.cfg &
	sleep 0.4
done

sleep 1

./tests/example_proposer 0 ../config.cfg &

cd -
