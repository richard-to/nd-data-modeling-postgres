# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL REFERENCES time (start_time),
    user_id BIGINT NOT NULL REFERENCES users (user_id),
    level TEXT NOT NULL,
    song_id TEXT REFERENCES songs (song_id),
    artist_id TEXT REFERENCES artists (artist_id),
    session_id BIGINT NOT NULL,
    location TEXT NOT NULL,
    user_agent TEXT NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    gender CHAR(1) CHECK (gender IN ('M', 'F')),
    level TEXT CHECK (level IN ('free', 'paid'))
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    artist_id TEXT NOT NULL REFERENCES artists (artist_id),
    year INT NOT NULL,
    duration DOUBLE PRECISION NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
""")

# - On conflict, update the record in case there are updates to the record
# - Granted this is somewhat arbitrary since it's not guaranteed that the
#   users will be inserted in order.
user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id)
DO
   UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        gender = EXCLUDED.gender,
        level = EXCLUDED.level;
""")

# - On conflict, update the record in case there are updates to the record
# - Granted this is somewhat arbitrary since it's not guaranteed that the
#   songs will be inserted in order.
song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id)
DO
   UPDATE SET
        title = EXCLUDED.title,
        artist_id = EXCLUDED.artist_id,
        year = EXCLUDED.duration;
""")

# - On conflict, update the record in case there are updates to the record
# - Granted this is somewhat arbitrary since it's not guaranteed that the
#   artists will be inserted in order.
artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id)
DO
   UPDATE SET
        name = EXCLUDED.name,
        location = EXCLUDED.location,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude;
""")

# On conflict, ignore duplicates since all values should be the same
time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time)
DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, s.artist_id
FROM songs s
JOIN artists a ON a.artist_id = s.artist_id
WHERE s.title = %s AND a.name = %s AND s.duration = %s
""")

# QUERY LISTS

# These create/drop queries must be run in the given order to ensure
# foreign key constraints are enforced properly.
create_table_queries = [
    user_table_create,
    artist_table_create,
    song_table_create,
    time_table_create,
    songplay_table_create,
]
drop_table_queries = [
    user_table_drop,
    artist_table_drop,
    song_table_drop,
    time_table_drop,
    songplay_table_drop,
]
