#!/bin/bash 

PROGNAME=${0##*/} 
# Options for script, in order given in LONGOPTS
CLEARDB="n"
PAXOS_DIR=/home/atomic/local/libpaxos
KILL="n"
NOREC="n"

SHORTOPTS="hckrp:"
LONGOPTS="help,clear-db,kill-all,no-rec,paxos-dir:"

usage() {
	echo "$0"
	echo "Long Opts: $LONGOPTS" 
	echo "Short Opts: $SHORTOPTS "
	exit 1
}

check_env () {
	if [ ! -e config/paxos_config.cfg ]; then
		echo "Please run this script from the root mosql-storage install folder"
		exit 1
	fi

	if [ ! -e $PAXOS_DIR/bin/example_acceptor ]; then
		echo "Provide a correct libpaxos directory, did not find acceptor"
		echo "in $PAXOS_DIR/bin"
		exit 1
	fi
}

ARGS=$(getopt -s bash --options $SHORTOPTS  \
  --longoptions $LONGOPTS --name $PROGNAME -- "$@" )

eval set -- "$ARGS"

while true; do
   case $1 in
      -h|--help)
         usage
         exit 0
         ;;
      -c|--clear-db) 
		#shift
		CLEARDB="y"
		;;
      -r|--no-rec) 
		#shift
		NOREC="y"
		;;
      -k|--kill-all) 
		#shift
		KILL="y"
		;;
      -p|--paxos) 
		shift
		echo "Setting Paxos dir to $1"
		PAXOS_DIR=$1
		;;
      *)
         break
         ;; 
	esac
	shift
done

#exit 1

. scripts/launchers.sh

check_env

echo "SIGINT'ing all procs"
killall -q -INT cm tapioca example_acceptor example_proposer rec
sleep 2

if [ "$KILL" = "y" ]; then
	echo "SIGKILL'ing all procs... they had two seconds."
	killall -q -9 cm tapioca example_acceptor example_proposer rec
fi

if [ "$CLEARDB" = "y" ]; then
	echo "Clearing DB folders before launch"
	rm -rf /tmp/acceptor_*
	rm -rf /tmp/rlog_*
fi


launch_acceptors
launch_proposers

sleep 2

if [ "$NOREC" = "n" ]; then
	launch_rec_nodes
fi

sleep 2

launch_cm
launch_nodes