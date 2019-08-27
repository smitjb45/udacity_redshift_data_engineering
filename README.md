Project: Data Modeling with Postgres
===================================

In this project, I applied what I learned on data warehousing with Redshift and build an ETL pipeline using SQL and Python. I defined fact and dimension tables for a star schema for a particular analytic focus, and wrote an ETL pipeline that transfers data from S3 bucket using Redshift using Python and SQL.

Pre-requisites
--------------

- Redshift
- AWS
- psycopg2 package

Getting Started
---------------

To get started, open a console window and type "python create_tables.py" then type "python etl.py"

Support
-------

- AWS: https://aws.amazon.com/redshift/resources/


The purpose of this database in the context of the startup, Sparkify, and their analytical goals.
-------

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Sparkify wants an engineer to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

State and justify your database schema design and ETL pipeline.
-------

I created the user table, song table, time table, and user tables to be my dimention tables and the song play to make the schema a Star design.

Please see table schema below:
-------

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