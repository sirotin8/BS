resource=`head -2 /usr/local/hadoop/etc/hadoop/masters | tail -1`
ssh $resource "$HADOOP_HOME/sbin/yarn-daemon.sh $1 resourcemanager"
ssh $resource "$HADOOP_HOME/sbin/yarn-daemon.sh $1 proxyserver"
