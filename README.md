# PROJECT-3: Data Warehouse

## Quick start

First, rename dwh_ref.cfg to dwh.cfg and fill in the open fields. Fill in AWS access key (KEY) and secret (SECRET).

To access AWS, you need to do the following steps in AWS:

1. Create an IAM user .
2. Create an IAM role with AmazonS3ReadOnlyAccess access rights.
3. Get the ARN of the IAM role.
4. Create and run a Redshift cluster.



Example data is provided in the data folder. To run the script and use that data, follow these steps:

1. Create an AWS S3 bucket.
2. Edit dwh.cfg: add your S3 bucket name in the LOG_PATH and SONG_PATH variables.
3. Copy the log_data and song_data folders to your own S3 bucket.

After installing Python 3 and the AWS SDK (boto3) libraries and dependencies, run the following commands from the command line:

1. `python3 create_tables.py` (to create the DB on AWS Redshift)
2. `python3 etl.py` (to process all the input data and load it into the DB)

---

## Overview

Project-3 deals with the data of a music streaming startup, Sparkify. The dataset consists of JSON files stored in AWS S3 buckets and contains two parts:

* **s3://udacity-dend/song_data**: static data about artists and songs
  Example of song data: `{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}`

* **s3://udacity-dend/log_data**: event data of service usage, including information about who listened to what song, when, where, and with which client
  ![Log-data example (log-data/2018/11/2018-11-12-events.json)](./Udacity-DEND-C3-Project3-LogDataExample-20190504.png)
* **s3://udacity-dend/log_json_path.json**: ...

Below are some statistics about the data set (results after running etl.py):

* s3://udacity-dend/song_data: 14897 files, 385252 DB rows
* s3://udacity-dend/log_data: 31 files, 8056 DB rows
* songplays: 245719 rows
* (unique) users: 104 rows
* songs: 384824 rows
* artists: 45266 rows
* time: 6813 rows

The project builds an ETL pipeline (Extract, Transform, Load) to create the DB and tables in an AWS Redshift cluster, fetch data from JSON files stored in AWS S3, process the data, and insert the data into the AWS Redshift DB. The technologies used in Project-3 include Python, SQL, AWS S3, and AWS Redshift DB.

---

## About the Database

The Sparkify analytics database (called sparkifydb) has a star schema design. A star design means that it has one Fact Table containing business data and supporting Dimension Tables. The Fact Table answers one of the key questions: what songs users are listening to. The DB schema is as follows:

![SparkifyDB schema as an ER Diagram](./Udacity-DEND-C3-Project3-ERD-20190517v1.png)

_*SparkifyDB schema as an ER Diagram.*_

### AWS Redshift Set-Up

AWS Redshift is used in the ETL pipeline as the DB solution. The set-up used in Project-3 is as follows:

* Cluster: 4x dc2.large nodes
* Location: US-West-2 (as Project-3's AWS S3 bucket)

### Staging Tables

* **staging_events**: event data indicating user actions (columns: event_id, artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId)
* **staging_songs**: song data containing information about songs and artists (columns: num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year)

Findings:

* song_data contained some anomalies that forced setting the artist_location, artist_name, and title columns to extra large (VARCHAR(500)).
* The COPY operation for event_data to the staging_events table is very fast (only a few seconds).
* The COPY operation for song_data to the staging_songs table is very slow due to the large amount of source data (the final test run took Redshift about 32 minutes for the analysis phase + 35 minutes for the actual copy operation).

### Fact Table

* **songplays**: song play data together with user, artist, and song info (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

### Dimension Tables

* **users**: user info (columns: user_id, first_name, last_name, gender, level)
* **songs**: song info (columns: song_id, title, artist_id, year, duration)
* **artists**: artist info (columns: artist_id, name, location, latitude, longitude)
* **time**: detailed time info about song plays (columns: start_time, hour, day, week, month, year, weekday)

Findings:

* Due to source data anomalies, the songs table contains one extra-large column (title VARCHAR(500)). Similarly, the artists table contains two extra-large columns (name VARCHAR(500), location VARCHAR(500)).
* The INSERT queries from the staging tables to the analytics tables are very fast and take only a few seconds each.

---

## HOWTO Use

**Project has two scripts:**

* **create_tables.py**: This script drops existing tables and creates new ones.
* **etl.py**: This script uses data from s3:/udacity-dend/song_data and s3:/udacity-dend/log_data, processes it, and inserts the processed data into the DB.


## Summary

In summary, Project-3 offers Sparkify, a music streaming startup, the necessary tools to analyze their service data. By using an ETL pipeline, the project extracts, transforms, and loads data from JSON files stored in AWS S3 into an AWS Redshift database. The data is organized into a star schema with a Fact Table and Dimension Tables, allowing Sparkify to answer key business questions such as "Who listened to which song and when?"

The project provides a step-by-step guide on how to set up the AWS environment, create the necessary IAM roles and clusters, and execute the ETL pipeline. The data is processed efficiently, and the resulting database contains meaningful insights about user interactions with the service, enabling Sparkify to make informed decisions and improve their music streaming platform.