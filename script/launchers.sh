VALGRIND_OPTIONS=" "
VALGRIND_EXTRA_OPTIONS=" --track-origins=yes --leak-check=full "
function do_killall () {
	local cmd="killall -INT dsmdb cm_node acceptor"
	echo "$cmd"
	$cmd > /dev/null
}

# New launchers

# Launchers accept two parameters - a prefix and suffix 
# (used for valgrind or adding logging)
launch_acceptors() {
	local cmd=""
	if [ "$1" = "valgrind" ]; then
		prefix="valgrind $VALGRIND_OPTIONS "
	fi
	for i in 2 1 0; do
		cmd="$prefix $PAXOS_DIR/bin/acceptor $i config/paxos_config.cfg "
		if [ "$2" = "log" ]; then
			$cmd > /tmp/acceptor$i.log 2>&1 &
		else 
			$cmd &
		fi
		sleep 0.1
	done
}

launch_proposers() {
	local cmd=""
	if [ "$1" = "valgrind" ]; then
		prefix="valgrind $VALGRIND_OPTIONS "
	fi
	cmd="$prefix $PAXOS_DIR/bin/proposer 0 config/paxos_config.cfg "
	if [ "$2" = "log" ]; then
		$cmd > /tmp/proposer0.log 2>&1  &
	else
		$cmd &
	fi
}

launch_rec_nodes () {
	local cmd=""
	if [ "$1" = "valgrind" ]; then
		prefix="valgrind $VALGRIND_OPTIONS "
	fi
	echo "Launching rec nodes"
	for i in 2 1 0; do
		cmd="$prefix bin/rec $i config/paxos_config.cfg "
		if [ "$2" = "log" ]; then
			$cmd > /tmp/rec_$i.log 2>&1  &
		else
			$cmd &
		fi
	done
}

launch_cm() {
	local cmd=""
	if [ "$1" = "valgrind" ]; then
		prefix="valgrind $VALGRIND_OPTIONS "
	fi
	cmd="$prefix bin/cm config/1.cfg config/paxos_config.cfg"
	if [ "$2" = "log" ]; then
		$cmd > /tmp/cm.log 2>&1  &
	else
		$cmd &
	fi
}

launch_nodes() {
	local cmd=""
	if [ "$1" = "valgrind" ]; then
		prefix="valgrind $VALGRIND_EXTRA_OPTIONS "
	fi
	cmd="$prefix bin/tapioca --ip-address $IP_ADDRESS --port 5555 --paxos-config "
	cmd="$cmd config/paxos_config.cfg  --storage-config config/1.cfg --node-type 0"; 
	if [ "$2" = "log" ]; then
		$cmd > /tmp/tapioca0.log 2>&1 &
	else
		$cmd &
	fi
}
