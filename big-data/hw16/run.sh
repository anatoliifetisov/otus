#!/usr/bin/env bash

path=${1-Train.csv}
directory=`dirname ${path}`
filename=`basename ${path}`

hdfs dfs -rm -r /hw16/input
hdfs dfs -rm -r /hw16/output

hdfs dfs -mkdir -p /hw16/input
hdfs dfs -mkdir -p /hw16/output
dfs dfs -put ${path} /hw16/input/${filename}

sbt clean && \
sbt assembly && \
spark-submit --class StackOverflowTagPredictor --driver-memory 10g target/scala-2.11/hw16-assembly-1.0.jar hdfs://localhost:9000/hw16/input/${filename}