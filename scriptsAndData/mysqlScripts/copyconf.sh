#!/bin/bash

for i in $(cat /home/mysql/bin/others);do
scp $mysqlfabriccfg $i:$MYSQL_HOME/etc
done
