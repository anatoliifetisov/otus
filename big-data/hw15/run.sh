#!/usr/bin/env bash

unzip -o ../hw9/vk.db.zip && \
rm -rf __MACOSX && \

sqlite3 -header -csv vk.db "select * from users;" | awk -F'"' 'NF&&NF%2==0{ORS=ORS==RS?" ":RS}1' > users.csv && \
sqlite3 -header -csv vk.db "select * from posts;" | awk -F'"' 'NF&&NF%2==0{ORS=ORS==RS?" ":RS}1' > posts.csv && \

hdfs dfs -rm -r -f /hw15/raw && \
hdfs dfs -mkdir -p /hw15/raw/users/ && \
hdfs dfs -mkdir -p /hw15/raw/posts/ && \

hdfs dfs -put -f users.csv /hw15/raw/users/users.csv && \
hdfs dfs -put -f posts.csv /hw15/raw/posts/posts.csv && \

mvn clean && \
mvn compile && \
mvn package && \

# registers uploaded CSVs as external tables
hive -f init_raw.hql && \
# registers ORC tables; users partitioned by age (derived from bdate) and clustered by id into 25 buckets, posts clustered by from_id into 25 buckets
hive -f init_ods.hql && \
# registers ORC table containing aggregated data; aggregation performed by joining two previous tables and flattening posts by user into a single string
hive -f init_ads.hql && \
# registeers two ORC tables; each one contains users who have sex/age filled and _any_ other not-NULL field 
hive -f init_dm.hql && \

hive -e "SELECT * FROM raw.users LIMIT 10;" > raw.users.txt && \
hive -e "SELECT * FROM raw.posts LIMIT 10;" > raw.posts.txt && \

hive -e "SELECT * FROM ods.users LIMIT 10;" > ods.users.txt && \
hive -e "SELECT * FROM ods.posts LIMIT 10;" > ods.posts.txt && \

hive -e "SELECT * FROM ads.users_with_posts LIMIT 10;" > ads.users_with_posts.txt && \

hive -e "SELECT * FROM dm.users_with_sex_and_data LIMIT 10;" > dm.users_with_sex_and_data.txt && \
hive -e "SELECT * FROM dm.users_with_age_and_data LIMIT 10;" > dm.users_with_age_and_data.txt