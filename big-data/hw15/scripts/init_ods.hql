CREATE DATABASE IF NOT EXISTS ods;
USE ods;

DELETE jars;
ADD JAR target/hw15-1.0.jar;
CREATE TEMPORARY FUNCTION bdate_to_age AS 'BdateToAgeUDF';
CREATE TEMPORARY FUNCTION nullify AS 'NullIfEmptyStringUDF';
SET hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS users;

CREATE TABLE users(
  id INT,
  age INT,
  activities STRING,
  interests STRING,
  music STRING,
  movies STRING,
  tv STRING,
  books STRING,
  games STRING,
  about STRING,
  quotes STRING)
COMMENT "Users with age in ORC format"
PARTITIONED BY (sex INT)
CLUSTERED BY (id) SORTED BY (id) INTO 25 BUCKETS
STORED AS ORC
LOCATION "/hw15/ods/users/";

INSERT INTO TABLE users PARTITION (sex)
SELECT id, bdate_to_age(nullify(bdate)), nullify(activities), nullify(interests), nullify(music), nullify(movies), 
       nullify(tv), nullify(books), nullify(games), nullify(about), nullify(quotes), sex
FROM raw.users;


DROP TABLE IF EXISTS posts;

CREATE TABLE posts(
  from_id INT,
  post STRING)
COMMENT "Posts in ORC format"
CLUSTERED BY (from_id) SORTED BY (from_id) INTO 25 BUCKETS
STORED AS ORC
LOCATION "/hw15/ods/posts/";

INSERT INTO TABLE posts
SELECT from_id, nullify(post)
FROM raw.posts;