#!/bin/bash

cmd=$1

case $cmd in
data)

cmd2=$2
case "$cmd2" in 

start)
for i in $(cat /home/mysql/bin/slaves);do
echo $i
(ssh $i "/usr/local/mysql/bin/mysqld_safe $mysqlfabric &") &
done
;;

stop)
for i in $(cat /home/mysql/bin/slaves);do
echo $i
ssh $i "/usr/local/mysql/bin/mysqladmin -uroot shutdown"
done
;;

esac
;;

backing)
cmd2=$2

case "$cmd2" in
start)
/usr/local/mysql/bin/mysqld_safe $mysqlfabric --skip-grant-tables &
;;
stop)
mysqladmin -uroot shutdown
;;
esac
;;

vmvia[1-5])
cmd2=$2
case $cmd2 in
start)
(ssh $cmd "/usr/local/mysql/bin/mysqld_safe $mysqlfabric &") &
;;
stop)
ssh $cmd "/usr/local/mysql/bin/mysqladmin -uroot shutdown"
;;
esac
;;

esac

