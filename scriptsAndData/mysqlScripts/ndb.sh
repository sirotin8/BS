#!/bin/bash

cmd=$1

case $cmd in
data)

cmd2=$2
case "$cmd2" in 

start)
for i in $(cat /home/mysql/bin/slaves);do
echo $i
(ssh $i /usr/local/mysql/bin/ndbd $mysqldefault $3)
done
;;

stop)
for i in $(cat /home/mysql/bin/slaves);do
echo $i
done
ndb_mgm -e "all stop"
;;

esac
;;

sql)
cmd2=$2

case "$cmd2" in
start)
/usr/local/mysql/bin/mysqld_safe $mysqldefault $3 &
;;
stop)
mysqladmin -uroot shutdown
;;
esac
;;

mgm)
cmd2=$2

case "$cmd2" in
start)

ndb_mgmd $mysqlf $3 $4
;;

stop)
ndb_mgm -e "1 stop"
;;

esac
;;

vmvia[2-5])
cmd2=$2
case $cmd2 in
start)
(ssh $i /usr/local/mysql/bin/ndbd $mysqldefault $1)
;;
stop)
ssh $cmd "/usr/local/mysql/bin/mysqladmin -uroot shutdown"
;;
esac
;;

stop)
/home/mysql/bin/ndb.sh sql stop
/home/mysql/bin/ndb.sh data stop
/home/mysql/bin/ndb.sh mgm stop

;;

start)
/home/mysql/bin/ndb.sh mgm start
/home/mysql/bin/ndb.sh data start

;;

esac

