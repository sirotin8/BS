#!/bin/bash

cat << EOF >> ~/.bashrc

unset CFLAGS
CFLAGS="\$CFLAGS `mysql_config --cflags`"
CFLAGS="\$CFLAGS `mysql_config --include`/storage/ndb"
CFLAGS="\$CFLAGS `mysql_config --include`/storage/ndb/ndbapi"
CFLAGS="\$CFLAGS `mysql_config --include`/storage/ndb/mgmapi"

unset LDFLAGS
LDFLAGS="\$LDFLAGS `mysql_config --libs_r`"
LDFLAGS="\$LDFLAGS -lndbclient"

EOF
source ~/.bashrc

