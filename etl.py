import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, LongType as Long


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates and returns a spark session to process data
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads json song data from s3 bucket specified in input_data
    Creates song and artist dataframe by selecting required columns
    Cleans the data by dropping any duplicates,renaming columns if required
    Writes this cleaned data in parquet format to s3 bucket specified in output_data
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    #song_data = input_data + 'song_data/A/A/*/*.json'
    
    # define schema for song data
    songSchema = R([
        Fld('artist_id',Str()),
        Fld('artist_latitude',Dbl()),
        Fld('artist_location',Str()),
        Fld('artist_longitude',Dbl()),
        Fld('artist_name',Str()),
        Fld('duration',Dbl()),
        Fld('num_songs',Int()),
        Fld('song_id',Str()),
        Fld('title',Str()),
        Fld('year',Int())
    ])
    
    # read song data file
    df = spark.read.json(song_data,schema=songSchema)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id','year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_output = os.path.join(output_data, 'songs/songs.parquet')
    songs_table.write.partitionBy('year', 'artist_id').parquet(songs_output, 'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id','artist_name','artist_location','artist_latitude','artist_longitude').dropDuplicates()
    
    # rename columns in artist_table
    artists_table = artists_table.withColumnRenamed('artist_name','name')\
                             .withColumnRenamed('artist_location','location')\
                             .withColumnRenamed('artist_latitude','latitude')\
                             .withColumnRenamed('artist_longitude','longitude')
    
    # write artists table to parquet files
    artists_output = os.path.join(output_data, 'artists/artists.parquet')
    artists_table.write.parquet(artists_output,'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Reads json log data from s3 bucket specified in input_data
    Creates users_table, time_table by selecting specific columns 
    Creates songplays dataframe by joining and then selecting columns from 
    log data and song data
    Writes users_table, time_table, and songplays_table in parquet format to
    s3 bucket specified in output_data
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df =  df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select('userId','firstName','lastName','gender','level').dropDuplicates()
    
    # rename columns in users_table
    users_table = users_table.withColumnRenamed('userId','user_id')\
                             .withColumnRenamed('firstName','first_name')\
                             .withColumnRenamed('lastName','last_name')
    
    # write users table to parquet files
    users_output = os.path.join(output_data, 'users/users.parquet')
    users_table.write.parquet(users_output,'overwrite')
  
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x : datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('start_time', get_datetime(df.ts)) 
    
    # extract columns to create time table
    time_table = df.select(df.start_time.alias('start_time'), \
                hour(df.start_time).alias('hour'),\
                dayofmonth(df.start_time).alias('day'),\
                weekofyear(df.start_time).alias('week'),\
                month(df.start_time).alias('month'),\
                year(df.start_time).alias('year'),\
                dayofweek(df.start_time).alias('weekday')).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_output = os.path.join(output_data, 'time/time.parquet')
    time_table.write.partitionBy('year', 'month').parquet(time_output, 'overwrite')

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(output_data + 'songs/*/*/*')
    
    # read in artists data to use for songplays table
    artists_df = spark.read.parquet(output_data + 'artists/*')
    
    # join song dataframe,artist dataframe and log dataframe
    songs_logs = df.join(songs_df, (df.song == songs_df.title))
    artists_songs_logs = songs_logs.join(artists_df, (songs_logs.artist == artists_df.name))
    
    songplays = artists_songs_logs.join(\
        time_table,\
        artists_songs_logs.ts == time_table.start_time, 'left'\
    )
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songplays.select(\
                                  df.start_time,\
                                  col('userId').alias('user_id'),\
                                  df.level,\
                                  songs_df.song_id,\
                                  artists_df.artist_id,\
                                  col('sessionId').alias('session_id'),\
                                  df.location,\
                                  col('userAgent').alias('user_agent'),\
                                  year(df.start_time).alias('year'),\
                                  month(df.start_time).alias('month'))\
                                  .withColumn('songplay_id', monotonically_increasing_id())


    # write songplays table to parquet files partitioned by year and month
    songplays_output = os.path.join(output_data, 'songplays/songplays.parquet')
    songplays_table.write.partitionBy("year", "month").parquet(songplays_output,'overwrite')


def main():
    """
    Initiallize input and output data paths
    Calls process functions for song and log data
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-data-lake-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
