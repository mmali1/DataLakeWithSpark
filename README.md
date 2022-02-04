# Project: Data Lake

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Purpose of this project is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Datasets:

**1. Song data:**

Song data resides in an s3 bucket located at `s3://udacity-dend/song_data`. 
Each file in this dataset is in JSON format and contains metadata about song and artist of that song. 
The files are partitioned by the first three ltters of each song's track ID. 

* Example: 

  * `song_data/A/B/C/TRABCEI128F424C983.json`
  * `song_data/A/A/B/TRAABJL12903CDCF1A.json`
  
A single song file looks like below:
                
```
{
    "num_songs": 1, 
    "artist_id": "ARJIE2Y1187B994AB7", 
    "artist_latitude": null, 
    "artist_longitude": null, 
    "artist_location": "", 
    "artist_name": "Line Renaud", 
    "song_id": "SOUPIRU12A6D4FA1E1", 
    "title": "Der Kleine Dompfaff", 
    "duration": 152.92036, 
    "year": 0
}
```

**2. Log data:**

Log data resides in an s3 bucket located at `s3://udacity-dend/log_data`.
The log files in the dataset are partitioned by year and month. 

* Example

  * `log_data/2018/11/2018-11-12-events.json`
  * `log_data/2018/11/2018-11-13-events.json`
            
## Schema for songplay analysis

Using the song and log datasets, we create a star schema optimized for queries on song play analysis.
This includes the following tables.

**Fact table**

1. **songplays**: records in log data associated with song plays i.e. records with page `NextSong`
  * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
  
**Dimension tables**

2. **users**: users in the app
  * user_id, first_name, last_name, gender, level
  
3. **songs**: songs in music database
  * song_id, title, artist_id, year, duration
  
4. **artists**: artists in music database
  * artist_id, name, location, lattitude, longitude
  
5. **time**: timestamps of records in `songplays` broken down into specific units
  * start_time, hour, day, week, month, year, weekday
  
## Scripts:

**1. etl.py**

etl.py reads data from s3, processes it and writes output data in parquet format back to s3.

**2. etl_dev.ipynb**

This notebook was mainly created to explore dataset on smaller scale.Code snippets from this notebook are integrated in etl.py. 
        

## How to run scripts:

1. create an **EMR cluster**
2. ssh to master node
3. run following command in terminal: 

`spark-submit --master yarn etl.py`

( When you run this script for firsttime if there is an error that says `No module named configparser`, 
try running this command interminal `pip install --user configparser`)
4. Once the script is finished, terminate cluster
5. Output files in parquet format are written to s3 bucket specified in `output_data` in `etl.py`. 
            

