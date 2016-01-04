for host in $(cat $HADOOP_HOME/etc/hadoop/slaves)
do
echo $host
ssh $host "$HADOOP_HOME/sbin/yarn-daemon.sh --config $HADOOP_HOME/etc/hadoop $1 nodemanager" &
done
wait
