import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# Staging tables
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
                
                artist      VARCHAR,
                auth        VARCHAR,
                firstName   VARCHAR,
                gender      VARCHAR,
                itemInSession VARCHAR,
                lastName    VARCHAR,
                length      VARCHAR,
                level       VARCHAR,
                location    VARCHAR,
                method      VARCHAR,
                page        VARCHAR,
                registration VARCHAR,
                sessionId   INTEGER   SORTKEY DISTKEY,
                song        VARCHAR,
                status      INTEGER,
                ts          BIGINT,
                userAgent   VARCHAR,
                userId      INTEGER                  
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
                num_songs           INTEGER,
                artist_id           VARCHAR  SORTKEY DISTKEY,
                artist_latitude     DECIMAL(9),
                artist_longitude    DECIMAL(9),
                artist_location     VARCHAR(500),
                artist_name         VARCHAR(500),
                song_id             VARCHAR,
                title               VARCHAR(500),
                duration            DECIMAL(9),
                year                INTEGER         
    );
""")

# Analytics tables
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
                songplay_id INTEGER IDENTITY(0,1)   PRIMARY KEY,
                start_time  TIMESTAMP               NOT NULL,
                user_id     VARCHAR(50)             NOT NULL DISTKEY,
                level       VARCHAR(10),
                song_id     VARCHAR(40)             NOT NULL,
                artist_id   VARCHAR(50)             NOT NULL,
                session_id  VARCHAR(50)             NOT NULL,
                location    VARCHAR(100),
                user_agent  VARCHAR(255)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
                user_id     INTEGER                 PRIMARY KEY,
                first_name  VARCHAR(50)             NOT NULL,
                last_name   VARCHAR(80)             NOT NULL,
                gender      VARCHAR(10),
                level       VARCHAR(10)
    ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
                song_id     VARCHAR(50)             PRIMARY KEY,
                title       VARCHAR(500)            NOT NULL,
                artist_id   VARCHAR(50)             NOT NULL,
                year        INTEGER,
                duration    DECIMAL(9)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
                artist_id   VARCHAR(50)             PRIMARY KEY,
                name        VARCHAR(500)            NOT NULL,
                location    VARCHAR(500),
                latitude    DECIMAL(9),
                longitude   DECIMAL(9)
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
                start_time  TIMESTAMP               PRIMARY KEY,
                hour        SMALLINT,
                day         SMALLINT                NOT NULL,
                week        SMALLINT,
                month       SMALLINT,
                year        SMALLINT                NOT NULL,
                weekday     SMALLINT
    ) diststyle all;
""")


# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ARN)


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (             start_time,
                                        user_id,
                                        level,
                                        song_id,
                                        artist_id,
                                        session_id,
                                        location,
                                        user_agent)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'   AS start_time,
            se.userId                   AS user_id,
            se.level                    AS level,
            ss.song_id                  AS song_id,
            ss.artist_id                AS artist_id,
            se.sessionId                AS session_id,
            se.location                 AS location,
            se.userAgent                AS user_agent
    FROM staging_events AS se
    JOIN staging_songs AS ss
    ON (se.artist = ss.artist_name AND se.song = ss.title)
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (                 user_id,
                                        first_name,
                                        last_name,
                                        gender,
                                        level)
    SELECT  DISTINCT userId           AS user_id,
             firstName                AS first_name,
             lastName                 AS last_name,
             gender                   AS gender,
             level                    AS level
    FROM staging_events 
    WHERE  page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (                 song_id,
                                        title,
                                        artist_id,
                                        year,
                                        duration)
    SELECT  DISTINCT song_id         AS song_id,
            title                    AS title,
            artist_id                AS artist_id,
            year                     AS year,
            duration                 AS duration
    FROM staging_songs ;
""")

artist_table_insert = ("""
    INSERT INTO artists (               artist_id,
                                        name,
                                        location,
                                        latitude,
                                        longitude)
    SELECT  DISTINCT  artist_id       AS artist_id,
             artist_name              AS name,
             artist_location          AS location,
             artist_latitude          AS latitude,
             artist_longitude         AS longitude
    FROM staging_songs ;
""")

time_table_insert = ("""
    INSERT INTO time (                  start_time,
                                        hour,
                                        day,
                                        week,
                                        month,
                                        year,
                                        weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + ts/1000 \
                * INTERVAL '1 second'        AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM    staging_events                   
    WHERE   page = 'NextSong';
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
