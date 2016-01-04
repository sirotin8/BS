namenode=`head -1 /usr/local/hadoop/etc/hadoop/masters`
ssh $namenode $HADOOP_HOME/sbin/hadoop-daemon.sh $1 namenode
ssh $namenode $HADOOP_HOME/sbin/hadoop-daemon.sh $1 secondarynamenode
