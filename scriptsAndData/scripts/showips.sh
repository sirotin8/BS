#!/bin/bash

 for i in 1 2 3 4 5;do ping -c 1 vmvia$i |grep PING;done

