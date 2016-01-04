#!/bin/bash

bin=${MYSQL_HOME}/bin

for i in 1 2 3 4 5;do
echo vmvia$i
ssh vmvia$i ps ax -eocmd |grep ^$bin/mysqld
ssh vmvia$i ps ax -eocmd |grep ^$bin/ndb
ssh vmvia$i ps ax -eocmd |grep ^ndb_mgm

done

