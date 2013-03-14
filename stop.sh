killall -INT tapioca cm acceptor rec
if [ "$1" == "-k" ]; then
	sleep 5
	CNT=`ps -ef | egrep "tapioca|cm|acceptor|rec" | grep -v egrep | wc -l`
	echo "Tapioca-related procs still alive $CNT"
	if [ $CNT -gt 0 ]; then
		echo "Killing with SIGKILL"
		killall -9 tapioca cm acceptor rec

	fi
fi

