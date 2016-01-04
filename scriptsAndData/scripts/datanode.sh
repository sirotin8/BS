for host in $(cat $HADOOP_HOME/etc/hadoop/slaves)
do
echo $host
ssh $host "$HADOOP_HOME/sbin/hadoop-daemon.sh $1 datanode" &
done
wait
