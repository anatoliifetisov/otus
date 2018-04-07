SET hive.exec.dynamic.partition.mode=nonstrict;

USE default;

DROP TABLE IF EXISTS settings;

CREATE TABLE settings(
  tag STRING
)
COMMENT "Tags to watch"
STORED AS ORC
LOCATION "/project/settings/";



CREATE DATABASE IF NOT EXISTS ods;
USE ods;

DROP TABLE IF EXISTS posts;

CREATE TABLE posts(
  id BIGINT,
  text STRING,
  lat DOUBLE,
  lng DOUBLE,
  sentiment INT
)
COMMENT "Cleaned posts with location (if present) and sentiment"
PARTITIONED BY (tag STRING)
STORED AS ORC
LOCATION "/project/ods/posts/";



CREATE DATABASE IF NOT EXISTS ads;
USE ads;

DROP TABLE IF EXISTS summary;

CREATE TABLE summary(
  tag STRING, 
  positive BIGINT,
  neutral BIGINT,
  negative BIGINT,
  total BIGINT,
  cumulative BIGINT,
  mean DOUBLE
)
COMMENT "Sentiment summary per tag"
STORED AS ORC
LOCATION "/project/ads/summary/";