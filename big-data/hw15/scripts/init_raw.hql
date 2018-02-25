CREATE DATABASE IF NOT EXISTS raw;
USE raw;


DROP TABLE IF EXISTS users;

CREATE EXTERNAL TABLE users(
  id INT,
  sex INT,
  bdate STRING,
  activities STRING,
  interests STRING,
  music STRING,
  movies STRING,
  tv STRING,
  books STRING,
  games STRING,
  about STRING,
  quotes STRING)
COMMENT "Initial import of users"
ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.OpenCSVSerde"
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar" = "\""
  )
STORED AS TEXTFILE
LOCATION "/hw15/raw/users/"
TBLPROPERTIES("skip.header.line.count"="1");


DROP TABLE IF EXISTS posts;

CREATE EXTERNAL TABLE posts(
  from_id INT,
  post STRING)
COMMENT "Initial import of post"
ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.OpenCSVSerde"
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar" = "\""
  )
STORED AS TEXTFILE
LOCATION "/hw15/raw/posts/"
TBLPROPERTIES("skip.header.line.count"="1");