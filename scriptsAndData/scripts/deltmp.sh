echo delete tmp files ...
for host in $(cat $HADOOP_HOME/etc/hadoop/slaves $HADOOP_HOME/etc/hadoop/masters)
do
echo $host
ssh $host rm -rf /tmp/hadoop*
done
