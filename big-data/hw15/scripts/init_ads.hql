CREATE DATABASE IF NOT EXISTS ads;
USE ads;

SET hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS users_with_posts;

CREATE TABLE users_with_posts(
  id INT,
  activities STRING,
  interests STRING,
  music STRING,
  movies STRING,
  tv STRING,
  books STRING,
  games STRING,
  about STRING,
  quotes STRING,
  posts STRING)
COMMENT "Aggregated data"
PARTITIONED BY (sex INT, age INT)
CLUSTERED BY (id) SORTED BY (id) INTO 25 BUCKETS
STORED AS ORC
LOCATION "/hw15/ads/users_with_posts/";

INSERT INTO TABLE users_with_posts PARTITION (sex, age)
SELECT u.id, u.activities, u.interests, u.music, u.movies, 
       u.tv, u.books, u.games, u.about, u.quotes, p.posts, u.sex, u.age
FROM ods.users AS u
LEFT JOIN (SELECT from_id, CONCAT_WS(' ', COLLECT_SET(post)) AS posts FROM ods.posts GROUP BY from_id) AS p
ON u.id = p.from_id;