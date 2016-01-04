#!/bin/bash

# fabric delete data

cmd=$1

case "$cmd" in
data)
for i in $(cat /home/mysql/bin/slaves);do
echo $i
ssh $i "cd /usr/local/mysql && rm -rf fabric/data && scripts/mysql_install_db --user=mysql --ldata=${MYSQL_HOME}/fabric/data"
done
;;

backing)
for i in $(cat /home/mysql/bin/masters);do
echo $i
ssh $i "cd /usr/local/mysql && rm -rf fabric/data && scripts/mysql_install_db --user=mysql --ldata=${MYSQL_HOME}/fabric/data"
done
;;

vmvia[1-5])
ssh $cmd "cd /usr/local/mysql && rm -rf fabric/data && scripts/mysql_install_db --user=mysql --ldata=${MYSQL_HOME}/fabric/data"
;;
esac


