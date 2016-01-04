
# local runterladen und dann per copyconf auf die anderen hosts die originale verteilen
cd $HADOOP_HOME/etc/hadoop
rm hdfs-site.xml yarn-site.xml
wget ftp://anonymous@vmvia1/hadoop/etc/multi/hdfs-site.xml
wget ftp://anonymous@vmvia1/hadoop/etc/multi/yarn-site.xml

~/bin/copyconf.sh

hdfs=$HADOOP_HOME/etc/hadoop/hdfs-site.xml
yarn=$HADOOP_HOME/etc/hadoop/yarn-site.xml

prefix=vmvia

for i in 1 2 3 4 5;do

host=$prefix$i

let "nmport=8040 + $i"
let "rmport=8080 + $i"
let "nnport=50070 + $i"
let "dnport=50080 + $i"
let "snnport=50090 + $i"

ssh $prefix$i bash -c "echo $host; ls $hdfs; ls $yarn; echo $host \
 echo $nmport $rmport $nnport $dnport $snnport; 
sed -i -e \"s/50075/$dnport/g\" $hdfs; 

sed -i -e \"s/8042/$nmport/g\" $yarn "
done 
