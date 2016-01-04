#!/bin/bash

cmd=$1

case "$cmd" in

data)
for i in $(cat /home/mysql/bin/slaves);do
ssh $i "wget ftp://anonymous@vmvia1/mysql/fabric/createuser.sql; /usr/local/mysql/bin/mysql -uroot < createuser.sql;rm createuser.sql"
done
;;

backing)
for i in $(cat /home/mysql/bin/masters);do
ssh $i "wget ftp://anonymous@vmvia1/mysql/fabric/createuser.sql; /usr/local/mysql/bin/mysql -uroot < createuser.sql;rm createuser.sql"
done
;;

vmvia[1-5])
ssh $cmd "wget ftp://anonymous@vmvia1/mysql/fabric/createuser.sql; /usr/local/mysql/bin/mysql -uroot < createuser.sql;rm createuser.sql"
;;

esac
