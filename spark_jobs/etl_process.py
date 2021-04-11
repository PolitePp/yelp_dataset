"""
Load json files to hdfs in raw format and then aggregate
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType


def load_info_about_reviews(spark_sess, hdfs_path):
    # Create structure of data. I added info about stars because
    # we can have nulls in this field, so we should filter them
    # and don't count in report
    schema_reviews = StructType([StructField('business_id', StringType(), nullable=False)
                                , StructField('review_id', StringType(), nullable=False)
                                , StructField('stars', StringType(), nullable=True)
                                , StructField('date', StringType(), nullable=True)
                                 ])
    df_reviews = spark_sess.read \
        .json('{0}/raw/review/yelp_academic_dataset_review.json'.format(hdfs_path), schema=schema_reviews)
    df_reviews.createOrReplaceTempView('reviews')

    # Load clear data about reviews.
    # In data was date with time so i decided to transform them
    # I added stars for future analysis if we will want to get medium stars
    spark.sql('''
    select b.business_id
            , r.review_id
            , cast(r.stars as int) as stars
            , trunc(to_date(substr(r.date, 1, 10), 'yyyy-MM-dd'), 'week') as date_trunc_week
    from business b
    inner join reviews r
    on b.business_id = r.business_id
    where r.date != ''
        and r.stars != ''
    ''').write.mode('overwrite').parquet('{0}/cleaned/review/review_data_success.parquet'.format(hdfs_path))

    # Load data with errors to analyze them
    # Example of errors - reviews with wrong business id, without stars or without date
    spark.sql('''
    select b.business_id
            , r.review_id
            , cast(r.stars as int) as stars
            , trunc(to_date(substr(r.date, 1, 10), 'yyyy-MM-dd'), 'week') as date_trunc_week
    from reviews r
    left join business b
    on r.business_id = b.business_id 
    where b.business_id is null or r.stars = '' or r.date = ''
    ''').write.mode('overwrite').parquet('{0}/cleaned/review/review_data_with_errors.parquet'.format(hdfs_path))


def load_info_about_checkins(spark_sess, hdfs_path):
    # Create structure of data
    schema_checkins = StructType([StructField('business_id', StringType(), nullable=False)
                                  , StructField('date', StringType(), nullable=True)
                                  ])
    df_checkins = spark_sess.read \
        .json('{0}/raw/checkin/yelp_academic_dataset_checkin.json'.format(hdfs_path), schema=schema_checkins)
    # Split column with multiple dates into rows
    df_checkins = df_checkins.withColumn('date', explode(split('date', ', ')))
    df_checkins.createOrReplaceTempView('checkins')

    # Load clear data about checkins.
    # We don't need info about time so i cut time and round it to week
    spark.sql('''
    select b.business_id
            , trunc(to_date(substr(c.date, 1, 10), 'yyyy-MM-dd'), 'week') as date_trunc_week
    from business b
    inner join checkins c
    on b.business_id = c.business_id
    where c.date != ''
    ''').write.mode('overwrite').parquet('{0}/cleaned/checkin/checkin_data_success.parquet'.format(hdfs_path))

    # Load data with errors to analyze them
    # Example of errors - checkins with wrong business id
    spark.sql('''
    select b.business_id
            , trunc(to_date(substr(c.date, 1, 10), 'yyyy-MM-dd'), 'week') as date_trunc_week
    from checkins c
    left join business b
    on c.business_id = b.business_id
    where b.business_id is null
            or c.date = ''
    ''').write.mode('overwrite').parquet('{0}/cleaned/checkin/checkin_data_errors.parquet'.format(hdfs_path))


def load_aggregated_info(spark_sess, hdfs_path):
    df_reviews_agg_by_week = spark_sess.read\
        .parquet('{0}/cleaned/review/review_data_success.parquet'.format(hdfs_path))\
        .groupBy("business_id", "date_trunc_week")\
        .agg(count("review_id").alias("count_stars_by_week")
             , avg("stars").alias("medium_stars_by_week"))

    df_checkins_agg_by_week = spark_sess.read\
        .parquet('{0}/cleaned/checkin/checkin_data_success.parquet'.format(hdfs_path))\
        .groupBy("business_id", "date_trunc_week")\
        .agg(count("date_trunc_week").alias("count_checkins_by_week"))

    df_reviews_overall = spark_sess.read \
        .parquet('{0}/cleaned/review/review_data_success.parquet'.format(hdfs_path)) \
        .groupBy("business_id") \
        .agg(count("review_id").alias("count_stars_overall")
             , avg("stars").alias("medium_stars_overall"))\

    df_checkins_overall = spark_sess.read \
        .parquet('{0}/cleaned/checkin/checkin_data_success.parquet'.format(hdfs_path)) \
        .groupBy("business_id") \
        .agg(count("business_id").alias("count_checkins_overall"))\

    df_reviews_agg_by_week.createOrReplaceTempView('reviews_agg_by_week')
    df_checkins_agg_by_week.createOrReplaceTempView('checkins_agg_by_week')
    df_reviews_overall.createOrReplaceTempView('reviews_overall')
    df_checkins_overall.createOrReplaceTempView('checkins_overall')

    # To analyse whole data we start from business table
    # which include all businesses (without checkins and reviews too)
    spark.sql('''
    select coalesce(rabw.business_id, cabw.business_id) as business_id,
            b.name,
            coalesce(rabw.date_trunc_week, cabw.date_trunc_week) as date_trunc_week,
            coalesce(rabw.count_stars_by_week, 0) as count_stars_by_week,
            coalesce(rabw.medium_stars_by_week, 0) as medium_stars_by_week,
            coalesce(cabw.count_checkins_by_week, 0) as count_checkins_by_week,
            coalesce(ro.count_stars_overall, 0) as count_stars_overall,
            coalesce(ro.medium_stars_overall, 0) as medium_stars_overall,
            coalesce(co.count_checkins_overall, 0) as count_checkins_overall
    from business b
    left join reviews_agg_by_week rabw
    on b.business_id = rabw.business_id
    left join checkins_agg_by_week cabw
    on b.business_id = cabw.business_id
        and rabw.date_trunc_week = cabw.date_trunc_week
    left join checkins_overall co
    on b.business_id = co.business_id 
    left join reviews_overall ro
    on b.business_id = ro.business_id
    order by 2, 3
    ''').coalesce(1).write.mode('overwrite').csv('{0}/aggregated/aggregated.csv'.format(hdfs_path), header='true')


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName('load_raw_level') \
        .master('spark://spark-master:7077') \
        .getOrCreate()

    common_path = 'hdfs://namenode:9000/'

    schema_business = StructType([StructField('business_id', StringType(), nullable=False)
                                  , StructField('name', StringType(), nullable=False)])
    df_business = spark.read \
        .json('{0}/raw/business/yelp_academic_dataset_business.json'.format(common_path), schema=schema_business) \
        .cache()
    df_business.createOrReplaceTempView('business')

    load_info_about_reviews(spark, common_path)
    load_info_about_checkins(spark, common_path)
    load_aggregated_info(spark, common_path)

    spark.stop()
