import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

ARN = config['IAM_ROLE']['ARN']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay;"
user_table_drop = "DROP TABLE IF EXISTS dim_user;"
song_table_drop = "DROP TABLE IF EXISTS dim_song;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
num_songs INTEGER,
artist_id VARCHAR,
artist_latitude DOUBLE PRECISION,
artist_longitude DOUBLE PRECISION,
artist_location TEXT,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration DOUBLE PRECISION,
year INTEGER
)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR,
    gender CHAR,
    item_in_session INTEGER,
    last_name VARCHAR,
    length DOUBLE PRECISION,
    level VARCHAR,
    location TEXT,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    session_id INTEGER,
    song TEXT,
    status INTEGER,
    ts BIGINT,
    user_agent TEXT,
    user_id INTEGER
)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS fact_songplay(
songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
start_time TIMESTAMP, 
user_id INTEGER, 
level VARCHAR, 
song_id VARCHAR, 
artist_id VARCHAR,
session_id INTEGER,
location VARCHAR,
user_agent TEXT
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS dim_user(
user_id INTEGER PRIMARY KEY,
first_name VARCHAR,
last_name VARCHAR,
gender CHAR(1),
level VARCHAR
);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS dim_song(
song_id VARCHAR PRIMARY KEY,
title TEXT,
artist_id VARCHAR,
year INTEGER,
duration DOUBLE PRECISION
);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS dim_artist(
artist_id VARCHAR PRIMARY KEY,
name VARCHAR,
location TEXT,
latitude DOUBLE PRECISION,
longitude DOUBLE PRECISION
);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS dim_time(
start_time TIMESTAMP PRIMARY KEY,
hour INTEGER,
day INTEGER,
week INTEGER,
month INTEGER,
year INTEGER,
weekday INTEGER
);
""")

# STAGING TABLES
staging_events_copy = ("""copy staging_events from '{}'
credentials 'aws_iam_role={}'
format as json '{}'
compupdate off region 'us-west-2'
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""copy staging_songs from '{}'
credentials 'aws_iam_role={}'
format as json 'auto'
compupdate off region 'us-west-2'
""").format(SONG_DATA, ARN)


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplay (start_time, user_id, level,
song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time, e.user_id, e.level, s.song_id, s.artist_id, e.session_id, s.artist_location, e.user_agent
FROM staging_events e
JOIN staging_songs s ON (e.artist = s.artist_name)
     AND (e.song = s.title)
     AND (e.length = s.duration)
     WHERE e.page = 'NextSong' AND e.user_id IS NOT NULL;
""")

user_table_insert = ("""
INSERT INTO dim_user(user_id, first_name, last_name, gender, level)
SELECT DISTINCT(user_id), first_name, last_name, gender, level
FROM staging_events
WHERE user_id IS NOT NULL

""")

song_table_insert = ("""
INSERT INTO dim_song(song_id, title, artist_id, year, duration)
SELECT DISTINCT(song_id), title,artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO dim_artist(artist_id, name, location, latitude, longitude)
SELECT DISTINCT(artist_id), artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs

""")

time_table_insert = ("""
INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT (ts), EXTRACT(HOUR FROM ts),EXTRACT(DAY FROM ts),EXTRACT(WEEK FROM ts),
EXTRACT(MONTH FROM ts), EXTRACT(YEAR FROM ts), EXTRACT(WEEKDAY FROM ts)
FROM(
SELECT (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ') as ts
FROM staging_events)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
