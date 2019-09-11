import configparser


# CONFIG
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

ARN = config.get('IAM_ROLE', 'ARN')[1:-1]
LOG_DATA_SOURCE = config.get('S3', 'LOG_DATA')
LOG_DATA_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA_SOURCE = config.get('S3', 'SONG_DATA')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE staging_events (
        artist_name VARCHAR,
        auth VARCHAR,
        user_first_name VARCHAR,
        user_gender VARCHAR,
        item_in_session INT NOT NULL,
        user_last_name VARCHAR,
        length REAL,
        user_level VARCHAR,
        user_location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration REAL,
        session_id INT NOT NULL,
        song VARCHAR,
        status SMALLINT,
        ts BIGINT,
        user_agent VARCHAR,
        user_id INT,
        PRIMARY KEY(session_id, item_in_session)
    )""")

# same order of cols as in json but without the first key (num_songs)
staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        artist_id VARCHAR NOT NULL,
        artist_latitude REAL,
        artist_longitude REAL,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR NOT NULL,
        title VARCHAR NOT NULL,
        duration REAL,
        year SMALLINT,
        PRIMARY KEY(song_id)
    )""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id INT identity(0,1),
        start_time TIMESTAMP NOT NULL,
        user_id VARCHAR NOT NULL,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR,
        PRIMARY KEY(songplay_id),
        FOREIGN KEY(start_time) REFERENCES time(start_time),
        FOREIGN KEY(song_id) REFERENCES songs(song_id),
        FOREIGN KEY(artist_id) REFERENCES artists(artist_id),
        FOREIGN KEY(user_id) REFERENCES users(user_id))
        DISTKEY(user_id)
        SORTKEY(start_time)
    """)

user_table_create = ("""
    CREATE TABLE users (
        user_id VARCHAR NOT NULL,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR,
        PRIMARY KEY(user_id))
        DISTKEY(user_id)
    """)

song_table_create = ("""
   CREATE TABLE songs (
        song_id VARCHAR NOT NULL,
        title VARCHAR,
        artist_id VARCHAR,
        year SMALLINT,
        duration REAL,
        PRIMARY KEY(song_id),
        FOREIGN KEY(artist_id) REFERENCES artists(artist_id))
        DISTSTYLE ALL
    """)

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id VARCHAR NOT NULL,
        name VARCHAR,
        location VARCHAR,
        latitude REAL,
        longitude REAL,
        PRIMARY KEY(artist_id))
        DISTSTYLE ALL
    """)

time_table_create = ("""
    CREATE TABLE time (
        start_time TIMESTAMP NOT NULL,
        hour SMALLINT,
        day SMALLINT,
        week SMALLINT,
        month SMALLINT,
        year SMALLINT,
        weekday SMALLINT,
        PRIMARY KEY(start_time))
        DISTSTYLE ALL
    """)

# COPY DATA TO STAGING TABLES
# Information on errors during copy is kept in the Redshift's stl_load_errors table

staging_events_copy = ("""
    COPY staging_events
        FROM {}
        CREDENTIALS 'aws_iam_role={}'
        REGION 'us-west-2'
        FORMAT AS JSON {}
        MAXERROR AS 1000
    """).format(LOG_DATA_SOURCE, ARN, LOG_DATA_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs
        FROM {}
        CREDENTIALS 'aws_iam_role={}'
        REGION 'us-west-2'
        FORMAT AS JSON 'auto'
        MAXERROR AS 1000
    """).format(SONG_DATA_SOURCE, ARN)


# INSERT DATA INTO ANALYTICS TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT
            TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second',
            e.user_id,
            e.user_level,
            s.song_id,
            s.artist_id,
            e.session_id,
            e.user_location,
            e.user_agent
            FROM staging_events AS e 
            JOIN staging_songs AS s
                ON e.artist_name = s.artist_name AND
                e.song = s.title AND
                e.length = s.duration
            WHERE e.page = 'NextSong'
    """)

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT user_id, user_first_name, user_last_name, user_gender, user_level 
            FROM staging_events
            WHERE page = 'NextSong'
    """)

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT song_id, title, artist_id, year, duration
            FROM staging_songs
    """)

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
            FROM staging_songs
    """)

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT 
            DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS st, 
            EXTRACT(h from st), 
            EXTRACT(d from st), 
            EXTRACT(w from st), 
            EXTRACT(mon from st), 
            EXTRACT(y from st), 
            EXTRACT(weekday from st)
            FROM staging_events AS e
            WHERE page = 'NextSong'
    """)

# QUERY LISTS
# Note that the order of queries in the lists does matter due to the relations between the tables

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create,
                        song_table_create, time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [user_table_insert, artist_table_insert, song_table_insert, time_table_insert,
                        songplay_table_insert]
