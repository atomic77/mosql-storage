function run_acceptor () {
	local id=$1
	local cmd="$acceptor $id $paxos_config"
	echo "$cmd"
	
	$cmd > acceptor$id.log &
}


function run_acceptors () {
	for (( i = 3; i > 0; i-- )); do
		run_acceptor $i
	done
	sleep 1
}


function run_cm () {
	local config=$1
	local cmd="$cm $config $paxos_config"
	echo "$cmd"
	
	$cmd > cm.log &
}


function run_tapioca () {
	local id=$1
	local config=$2
    local port=$3
	local cmd="$tapioca -d $id $config $paxos_config $port"
	#local cmd="$tapioca $id $config $paxos_config $port"
	echo "$cmd"
	
	#valgrind --tool=exp-dhat  $cmd > tapioca$id.log 2> tapioca-dhat-tpcc-alt.out &
	#valgrind --track-origins=yes $cmd > tapioca$id.log 2> tapioca$id.log  &
	$cmd > tapioca$id.log &
	sleep 1
}


function do_killall () {
	local cmd="killall -INT dsmdb cm_node acceptor"
	echo "$cmd"
	$cmd > /dev/null
}
