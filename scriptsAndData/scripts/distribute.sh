
if [ $# -lt 1 ]
  then
    echo "No arguments supplied"
exit
fi

ver=$1

cd ~; mkdir -p tmp/myhadoopetc; cp -r /usr/local/hadoop/etc/hadoop/* tmp/myhadoopetc;
 

for i in $(cat $HADOOP_HOME/etc/hadoop/slaves $HADOOP_HOME/etc/hadoop/masters);do

ssh $i bash -c "cd ~; rm -rf tmphadoop$ver; mkdir tmphadoop$ver;cd tmphadoop$ver;
wget anonymous@vmvia1:/hadoop/setup/hadoop-${ver}.tar.gz;
tar -xzf hadoop-${ver}.tar.gz;
mkdir hadoop_old;
mv /usr/local/hadoop/* hadoop_old;
cp -r hadoop-${ver}/* /usr/local/hadoop";

done;

cp -r ~/myhadoopetc/* /usr/local/hadoop/etc/hadoop

copyconf.sh


