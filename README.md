# YELP dataset loading
As a data engineer, you should create a (small) data lake. The data lake is supposed to consist of raw, cleaned and aggregated Yelp data. The aggregation should include the stars per business on a weekly basis and the number of checkins of a business compared to the overall star rating

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

What things you need to install the software

```
Docker
Docker-compose
Git
```
Tested on docker version 20.10.5, docker-compose version 1.28.4

### Installing

Firstly, load this project into some folder using command

```
git clone https://github.com/PolitePp/yelp_dataset.git
```

Then create the network for docker containers

```
docker network create hadoopspark
```

Then go to the root directory of the project

```
cd yelp_dataset/
```

Load json files of yelp (https://www.kaggle.com/yelp-dataset/yelp-dataset)
into input_data directory. Need to log in

Then run docker-compose command in daemon mode and wait till running

```
docker-compose up -d
```

Load raw data into hdfs. I found 2 ways to do it. 
First is change Dockerfile of bde2020/spark-worker:3.1.1-hadoop3.2 
and add run .sh command in CMD section
Second is go inside the container and run sh file locally. 
I chose the second way because it's easy to realise 
It will create folders for raw data and load them into hdfs

```
docker exec -it namenode bash
sh /opt/input_data/load_data_to_hadoop.sh
```

Then exit from the container

```
exit
```

Let's check that we successfully loaded our data. 
Firstly, we need to find ip of namenode. To do this run command

```
docker network inspect hadoopspark
```

Find the block which connected with namenode. Example

```
"1c274225747858c417f74c2c0c4e5ee4f313bce66d80ff867e66f23d7271ed48": {
                "Name": "namenode",
                "EndpointID": "ed1f5479ca77d5e1446436ae3ce540c4336d8eb0d6fd052d0dc10cb3e8cd5f2e",
                "MacAddress": "02:42:ac:13:00:03",
                "IPv4Address": "172.19.0.3/16",
                "IPv6Address": ""
            },

```

Copy IPv4Address block before backslash (172.19.0.3, for example) 
and then go to the browser with link
```
172.19.0.3:9870
```

Then go to Utilities -> Browse the file system -> Press "Go" button, 
and you should see the directory called "raw". Inside this directory
we have directories for each json file

Let's run our spark job. Go inside spark-master container. 
All comments about loading in /spark_jobs/etl_process.py file
```
docker exec -it spark-master bash
/spark/bin/spark-submit /opt/hadoopspark/etl_process.py
```

Exit from the container
```
exit
```

Then go to browser and reload page with Browse directory. 
We should have 3 directories in root folder: raw, cleaned, aggregated.
I decided to store cleaned level in parquet files. 
Aggregated level stores in csv for easy watching. 
Go to aggregated folder -> aggregated.csv
Load *.csv file into local folder. Press on file and click download.
It will raise we error and in url we will have some symbols instead of ip. Example:

```
http://9096ff9bce9e:9864/webhdfs/v1/aggregated/aggregated.csv/part-00000-bc579814-1b86-44d3-b0c0-cddd2528d216-c000.csv?op=OPEN&namenoderpcaddress=namenode:9000&offset=0
```

I don't know how to fix it correctly, so go to the terminal and run

```
docker network inspect hadoopspark
```

Find information about datanode container. Example

```
"9096ff9bce9ed70c3096cfb6ea339b942619bca29fea0ac866af7125ca001ba1": {
                "Name": "datanode",
                "EndpointID": "1ff96de6b8737c97c6636d2575a31265d015c8a6c04940e4582f227650c1208f",
                "MacAddress": "02:42:ac:13:00:02",
                "IPv4Address": "172.19.0.2/16",
                "IPv6Address": ""
            },
```

Copy IPv4Address (172.19.0.2, for example). 
Go to the browser and replace symbols before the port with this IP
Example:
```
http://172.19.0.2:9864/webhdfs/v1/aggregated/aggregated.csv/part-00000-bc579814-1b86-44d3-b0c0-cddd2528d216-c000.csv?op=OPEN&namenoderpcaddress=namenode:9000&offset=0
```

Now we have csv file with columns:
* business_id - unique identifier of business
* name - name of business
* date_trunc_week - start of week 
* count_stars_by_week - count of stars per week and business
* medium_stars_by_week - medium star per week and business
* count_checkins_by_week - count of checkins per week and business
* count_stars_overall - count of stars per business
* medium_stars_overall - medium star per business
* count_checkins_overall - count of checkins per business