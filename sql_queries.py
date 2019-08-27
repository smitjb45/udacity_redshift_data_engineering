import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS logs_staging"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS songs_staging 
(
num_songs int
,artist_id varchar distkey
,artist_latitude double precision
,artist_longitude double precision
,artist_location varchar
,artist_name varchar
,song_id varchar sortkey
,title varchar
,duration double precision
,year int
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS logs_staging 
(
artist varchar
,auth varchar
,firstName varchar
,gender char
,itemInSession bigint
,lastName varchar
,length double precision
,level varchar
,location varchar
,method varchar
,page varchar
,registration double precision
,sessionId bigint
,song varchar
,status int
,ts bigint sortkey
,userAgent varchar
,userId int distkey
)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays 
                            (
                            songplay_id bigint IDENTITY(1, 1) distkey,
                            start_time bigint, 
                            user_id int, 
                            level varchar, 
                            song_id varchar, 
                            artist_id varchar,  
                            session_id int NOT NULL,
                            location varchar, 
                            user_agent varchar
                            )
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users 
                        (
                        user_id int NOT NULL distkey,
                        first_name varchar,
                        last_name varchar,
                        gender char, 
                        level varchar
                        )
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs 
                        (
                        song_id varchar distkey, 
                        title text,
                        artist_id varchar,
                        year int,
                        duration double precision sortkey
                        )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists 
                          (
                          artist_id varchar NOT NULL distkey, 
                          name varchar sortkey,
                          location varchar,
                          lattitude double precision,
                          longitude double precision
                    
                          )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time 
                        (
                        start_time varchar NOT NULL distkey, 
                        hour int, 
                        day int, 
                        week int, 
                        month int,
                        year int, 
                        weekday int
                        
                        )
""")

# STAGING TABLES

staging_events_copy = ("""
copy logs_staging from {} 
credentials {} 
FORMAT AS JSON {}
region 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'),config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY songs_staging FROM {}
credentials {} 
json 'auto'
region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'),config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays 
(
start_time
,user_id
,level
,song_id
,artist_id
,session_id
,location
,user_agent
)                            
WITH CTE_A
AS
(
SELECT 
a.song_id
,b.artist_id
,a.title
,b.name
,a.duration
FROM songs a
LEFT JOIN artists b ON a.artist_id = b.artist_id
)
SELECT
b.ts AS start_time
,b.userid AS user_id
,b.level
,a.song_id
,a.artist_id
,b.sessionid AS session_id
,b.location
,b.useragent AS user_agent
FROM CTE_A a
RIGHT JOIN logs_staging b 
ON a.title = b.song AND a.name = b.artist AND a.duration = b.length;
""")

user_table_insert = ("""
INSERT INTO users 
(
user_id
,first_name
,last_name
,gender
,level
) 
WITH CTE_A
AS
(
SELECT                        
userid, 
MAX(ts) AS ts
FROM
logs_staging
GROUP BY userid
)
SELECT 
a.userid 
,b.firstname
,b.lastname
,b.gender 
,b.level
FROM CTE_A a
INNER JOIN logs_staging b ON a.userid = b.userid AND a.ts = b.ts
ORDER BY a.userid;
""")

song_table_insert = ("""
INSERT INTO songs 
(
song_id
,title
,artist_id
,"year"
,duration
) 
SELECT 
song_id
,title
,artist_id
,"year"
,duration 
FROM songs_staging;
""")

artist_table_insert = ("""
INSERT INTO artists 
(
artist_id
,name
,location
,lattitude
,longitude
) 
SELECT
DISTINCT
artist_id
,artist_name
,artist_location
,artist_latitude
,artist_longitude 
FROM songs_staging;
""")

time_table_insert = ("""
INSERT INTO time 
(
start_time, 
hour, 
day, 
week, 
month, 
year, 
weekday
) 
SELECT
DISTINCT(ts), 
EXTRACT(HOUR from date_add('ms',ts,'1970-01-01')), 
EXTRACT(DAY from date_add('ms',ts,'1970-01-01')), 
EXTRACT(WEEK from date_add('ms',ts,'1970-01-01')), 
EXTRACT(MONTH from date_add('ms',ts,'1970-01-01')), 
EXTRACT(YEAR from date_add('ms',ts,'1970-01-01')), 
EXTRACT(DOW from date_add('ms',ts,'1970-01-01'))
FROM logs_staging;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [ user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]


                       
# WITH CTE_A
# AS
# (
# SELECT 
# a.song_id
# ,b.artist_id
# ,a.title
# ,b.name
# ,a.duration
# FROM songs a
# LEFT JOIN artists b ON a.artist_id = b.artist_id
# )
# SELECT
# b.ts AS start_time
# ,b.userid AS user_id
# ,b.level
# ,a.song_id
# ,a.artist_id
# ,b.sessionid AS session_id
# ,b.location
# ,b.useragent AS user_agent
# FROM CTE_A a
# RIGHT JOIN logs_staging b 
# ON a.title = b.song AND a.name = b.artist AND a.duration = b.length
# WHERE user_id IS NULL;






