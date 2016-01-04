#!/bin/bash

cmd=$1

case "$1" in

memory)
ndb_mgm -e "all report memoryusage"
;;
*)
ndb_mgm -e show
;;
esac

