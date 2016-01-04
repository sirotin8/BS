

pattern=$1

cp $HADOOP_HOME/etc/hadoop/yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml.$attern
resource=vmvia5
namenode=vmvia5
sed -i -e "s/$resource/$namenode/g" $HADOOP_HOME/etc/hadoop/yarn-site.xml
cp $HADOOP_HOME/etc/hadoop/yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml.single
cp $HADOOP_HOME/etc/hadoop/slaves $HADOOP_HOME/etc/hadoop/slaves.$pattern
cp $HADOOP_HOME/etc/hadoop/masters $HADOOP_HOME/etc/hadoop/masters.$pattern
echo $namenode > $HADOOP_HOME/etc/hadoop/slaves.single
echo -e $namenode\n$namenode > $HADOOP_HOME/etc/hadoop/masters.single
cp $HADOOP_HOME/etc/hadoop/slaves.single $HADOOP_HOME/etc/hadoop/slaves
cp $HADOOP_HOME/etc/hadoop/masters.single $HADOOP_HOME/etc/hadoop/masters
