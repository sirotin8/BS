
if [[ $1 == /* ]]; then p=$1; else p=/user/`whoami`/$1; fi

hdfs="`hdfs fsck $p -files -locations -blocks 2>/dev/null`"

rep=`echo "$hdfs"|grep Average|cut -f2`

echo "$hdfs"|tail -n20

blocks=`echo "$hdfs"|grep BP|sed -e's/[^\[]*\[\|,\|]//g' -e's/ /\n/g'`

declare -A a
for i in $blocks;do
let a[$i]++
let b++
done

echo
echo Blocks-distribution:
for i in ${!a[@]};do
let per="100 * ${a[$i]} / $b"
echo -e "Host: $i \n\tBlocks: ${a[$i]} \n\tPercent: $per%\n"
done
