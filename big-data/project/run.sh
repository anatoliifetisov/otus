#!/usr/bin/env bash
hdfs dfs -mkdir -p /project/ads/summary/
hdfs dfs -mkdir -p /project/ods/tweets/
hdfs dfs -mkdir -p /project/settings/

mkdir -p analysis/lib && cd analysis/lib && curl -LOJ -C - http://nlp.stanford.edu/software/stanford-corenlp-models-current.jar && cd ../..
mkdir -p analysis/lib && cd analysis/lib && curl -LOJ -C - http://nlp.stanford.edu/software/stanford-english-corenlp-models-current.jar && cd ../..

hive -f init.hql

echo "Running with parameters:"
cat configuration/src/main/resources/application.conf
echo "\n"

#it takes a lot of memory to pack models
export SBT_OPTS="-Xmx8192M -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -Xss2M"

sbt clean compile assembly &&\
spark-submit --class otus.project.streaming.TwitterStreamer --driver-memory 2g streaming/target/scala-2.11/streaming-assembly-1.0.jar &\
spark-submit --class otus.project.analysis.PostsAnalyzer --driver-memory 8g analysis/target/scala-2.11/analysis-assembly-1.0.jar &\
spark-submit --class otus.project.aggregation.PostsAggregator --driver-memory 2g aggregation/target/scala-2.11/aggregation-assembly-1.0.jar &\
java -jar web/target/scala-2.11/web-assembly-1.0.jar


