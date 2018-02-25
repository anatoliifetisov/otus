CREATE DATABASE IF NOT EXISTS dm;
USE dm;

SET hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS users_with_sex_and_data;

CREATE TABLE users_with_sex_and_data(
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
COMMENT "Users with sex and some features defined"
PARTITIONED BY (sex INT, age INT)
CLUSTERED BY (id) SORTED BY (id) INTO 25 BUCKETS
STORED AS ORC
LOCATION "/hw15/ads/users_with_sex_and_data/";

INSERT INTO TABLE users_with_sex_and_data PARTITION (sex, age)
SELECT u.id, u.activities, u.interests, u.music, u.movies, 
       u.tv, u.books, u.games, u.about, u.quotes, u.posts, u.sex, u.age
FROM ads.users_with_posts AS u
WHERE u.sex IS NOT NULL AND u.sex != 0 AND (u.activities IS NOT NULL OR u.interests IS NOT NULL OR u.music IS NOT NULL OR u.movies IS NOT NULL 
      OR u.tv IS NOT NULL OR u.books IS NOT NULL OR u.games IS NOT NULL OR u.about IS NOT NULL OR u.quotes IS NOT NULL OR u.posts IS NOT NULL);

DROP TABLE IF EXISTS users_with_age_and_data;

CREATE TABLE users_with_age_and_data(
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
COMMENT "Users with age and some features defined"
PARTITIONED BY (sex INT, age INT)
CLUSTERED BY (id) SORTED BY (id) INTO 25 BUCKETS
STORED AS ORC
LOCATION "/hw15/ads/users_with_age_and_data/";

INSERT INTO TABLE users_with_age_and_data PARTITION (sex, age)
SELECT u.id, u.activities, u.interests, u.music, u.movies, 
       u.tv, u.books, u.games, u.about, u.quotes, u.posts, u.sex, u.age
FROM ads.users_with_posts AS u
WHERE u.age IS NOT NULL AND (u.activities IS NOT NULL OR u.interests IS NOT NULL OR u.music IS NOT NULL OR u.movies IS NOT NULL 
      OR u.tv IS NOT NULL OR u.books IS NOT NULL OR u.games IS NOT NULL OR u.about IS NOT NULL OR u.quotes IS NOT NULL OR u.posts IS NOT NULL);