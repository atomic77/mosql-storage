#!/bin/bash 

PROGNAME=${0##*/} 
# Options for script, in order given in LONGOPTS
CLEARDB="n"
PAXOS_DIR=~/local/libpaxos
KILL="n"
NOREC="n"
NOPROP="n"
DELAY=2
VALGRIND="n"
LOG="n"

SHORTOPTS="hckrp:d:ovl"
LONGOPTS="help,clear-db,kill-all,no-rec,paxos-dir:delay:,no-proposer,valgrind,log"

usage() {
	echo "$0 <Options>"
	echo
	echo "-c, --clear-db"
	echo "	Remove all acceptor logs on restart"
	echo
	echo "-k, --kill-all"
	echo "	SIGKILL all mosql-storage/se/libpaxos processes before starting"
	echo
	echo "-r, --no-rec"
	echo "	Don't launch recovery nodes (useful in some testing scenarios)"
	echo
	echo "-p, --paxos-dir <dir>"
	echo "	Base installation directory of libpaxos"
	echo
	echo "-o, --no-proposer <dir>"
	echo "	Don't launch a proposer"
	echo
	echo "-v, --valgrind"
	echo "	Run processes with valgrind"
	echo
	echo "-l, --log"
	echo "	Send process output to /tmp/<proc>.log"
	exit 1
}

check_env () {
	if [ ! -e config/paxos_config.cfg ]; then
		echo "Please run this script from the root mosql-storage install folder"
		exit 1
	fi

	if [ ! -e $PAXOS_DIR/bin/acceptor ]; then
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
		CLEARDB="y"
		;;
      -o|--no-proposer) 
		NOPROP="y"
		;;
      -r|--no-rec) 
		NOREC="y"
		;;
      -k|--kill-all) 
		KILL="y"
		;;
      -d|--delay) 
		shift
		echo "Setting delay to $1"
		DELAY=$1
		;;
      -p|--paxos-dir) 
		shift
		echo "Setting Paxos dir to $1"
		PAXOS_DIR=$1
		;;
      -v|--valgrind) 
		VALGRIND="y"
		;;
      -l|--log) 
		LOG="y"
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
killall -q -INT cm tapioca acceptor proposer rec
#pkill -u $USER -f valgrind
sleep $DELAY

if [ "$KILL" = "y" ]; then
	echo "SIGKILL'ing all procs... they had $DELAY seconds."
	killall -q -9 cm tapioca acceptor proposer rec
#	pkill -9 -u $USER -f valgrind
fi

if [ "$CLEARDB" = "y" ]; then
	echo "Clearing DB folders before launch"
	rm -rf /tmp/acceptor_*
	rm -rf /tmp/rlog_*
fi

#set -x
params[0]='0'
params[1]='0'

if [ "$VALGRIND" = "y" ]; then
	params[0]="valgrind"
fi

if [ "$LOG" = "y" ]; then
	params[1]="log"
fi

launch_acceptors ${params[@]}

sleep $DELAY

if [ "$NOREC" = "n" ]; then
	launch_rec_nodes ${params[@]}
fi

if [ "$NOPROP" = "n" ]; then
	launch_proposers ${params[@]}
fi

sleep $DELAY

launch_cm  ${params[@]}
launch_nodes ${params[@]}
