for host in $(cat $HADOOP_HOME/etc/hadoop/slaves $HADOOP_HOME/etc/hadoop/masters | sort | uniq)
do
echo $host
ssh $host jps |grep -v Jps
done
