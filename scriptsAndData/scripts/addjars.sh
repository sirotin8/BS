
dir=$1

for f in $dir/*.jar; do
export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$f;
done

