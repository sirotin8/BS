
if [ "$(id -u)" != "0" ]; then
   echo "This script must be run as root" 1>&2
   exit 1
fi

user=$1

useradd -Um -s /bin/bash $user
passwd $user
cp /home/hadoop/.bashrc /home/$user

export HADOOP_USER_NAME=hadoop
/usr/local/hadoop/bin/hdfs dfs -mkdir /user/${user}
/usr/local/hadoop/bin/hdfs dfs -chown $user:$user /user/$user 

