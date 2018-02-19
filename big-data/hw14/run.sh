#!/usr/bin/env bash

url1=${1-https://dumps.wikimedia.org/other/clickstream/2018-01/clickstream-enwiki-2018-01.tsv.gz}
arch1=`basename ${url1}`
file1=${arch1%.*}

url2=${2-https://dumps.wikimedia.org/other/clickstream/2017-12/clickstream-enwiki-2017-12.tsv.gz}
arch2=`basename ${url2}`
file2=${arch2%.*}

curl -OJ -C - ${url1}  && \
gzip -dkf ${arch1}
curl -OJ -C - ${url2} && \
gzip -dkf ${arch2} && \

rm from_broadcast.txt
rm from_reduceside.txt

hdfs dfs -rm -r /hw14/input
hdfs dfs -rm -r /hw14/top10k
hdfs dfs -rm -r /hw14/outb
hdfs dfs -rm -r /hw14/outr
hdfs dfs -rm -r /hw14/outb_collected
hdfs dfs -rm -r /hw14/outr_collected

hdfs dfs -mkdir -p /hw14/input/ && \
hdfs dfs -put ${file1} /hw14/input/${file1} && \
hdfs dfs -put ${file2} /hw14/input/${file2} && \
mvn clean && \
mvn package && \
hadoop jar target/hw14-1.0.jar 1 /hw14/input/${file1} /hw14/top10k && \
hadoop jar target/hw14-1.0.jar 2 /hw14/top10k/part-r-00000 /hw14/input/${file2} /hw14/outb && \
hadoop jar target/hw14-1.0.jar 3 /hw14/top10k/part-r-00000 /hw14/input/${file2} /hw14/outr && \
hadoop jar target/hw14-1.0.jar 4 /hw14/outb /hw14/outb_collected && \
hadoop jar target/hw14-1.0.jar 4 /hw14/outr /hw14/outr_collected && \
hdfs dfs -get /hw14/outb_collected/part-r-00000 from_broadcast.txt && \
hdfs dfs -get /hw14/outr_collected/part-r-00000 from_reduceside.txt && \
echo Broadcast result && \
cat from_broadcast.txt && \
echo Reduce side result && \
cat from_reduceside.txt