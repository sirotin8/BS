echo cleanup ...
for host in $(cat $HADOOP_HOME/etc/hadoop/slaves $HADOOP_HOME/etc/hadoop/masters)
do
#echo \
scp -r $HADOOP_HOME/etc/hadoop $host:$HADOOP_HOME/etc
done
