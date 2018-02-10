#!/usr/bin/env bash
p=${1-alice30.txt}
directory=`dirname ${p}`
filename=`basename ${p}`

rm top200.txt
rm total.txt

hdfs dfs -rm -r /hw13/input
hdfs dfs -rm -r /hw13/pairs
hdfs dfs -rm -r /hw13/top200
hdfs dfs -rm -r /hw13/total

hdfs dfs -mkdir -p /hw13/input/ && \
hdfs dfs -put ${directory}/${filename} /hw13/input/${filename} && \
mvn clean && \
mvn package && \
hadoop jar target/hw13-1.0.jar 1 /hw13/input/${filename} /hw13/pairs && \
hadoop jar target/hw13-1.0.jar 2 /hw13/pairs/ /hw13/top200 && \
hadoop jar target/hw13-1.0.jar 3 /hw13/pairs/ /hw13/total && \
hdfs dfs -get /hw13/top200/part-r-00000 top200.txt && \
hdfs dfs -get /hw13/total/part-r-00000 total.txt && \
echo Top200 pairs occured in ${filename} && \
cat top200.txt && \
echo Total pairs in ${filename} && \
cat total.txt