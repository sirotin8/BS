#!/bin/bash

for i in $(cat /home/mysql/bin/this /home/mysql/bin/others);do
echo $i:
ssh $i "/usr/local/mysql/bin/mysqladmin -uroot status"
echo
done
