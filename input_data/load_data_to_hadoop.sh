#!/bin/bash
cd /opt/hadoop-3.2.1/bin
hdfs dfs -mkdir /raw
hdfs dfs -mkdir /raw/business
hdfs dfs -mkdir /raw/checkin
hdfs dfs -mkdir /raw/review
hdfs dfs -mkdir /raw/tip
hdfs dfs -mkdir /raw/user
hdfs dfs -put /opt/input_data/yelp_academic_dataset_business.json /raw/business
hdfs dfs -put /opt/input_data/yelp_academic_dataset_checkin.json /raw/checkin
hdfs dfs -put /opt/input_data/yelp_academic_dataset_review.json /raw/review
hdfs dfs -put /opt/input_data/yelp_academic_dataset_tip.json /raw/tip
hdfs dfs -put /opt/input_data/yelp_academic_dataset_user.json /raw/user