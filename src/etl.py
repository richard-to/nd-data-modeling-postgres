"""
ETL script to extract and transform Sparkify song and activity log data
into a Postgres database.
"""
import glob
import os

import pandas as pd
import psycopg2

from config import (
    SPARKIFY_DB_CONNECTION_STRING,
    LOG_DATA_PATH,
    SONG_DATA_PATH,
)
import sql_queries as sq


def process_song_file(cur, filepath):
    """Processes a song file by inserting data into the artists and songs tables

    Example file format:

        {
            "num_songs": 1,
            "artist_id": "ARD7TVE1187B99BFB1",
            "artist_latitude": null,
            "artist_longitude": null,
            "artist_location": "California - LA",
            "artist_name": "Casual",
            "song_id": "SOMZWCG12A8C13C480",
            "title": "I Didn't Mean To",
            "duration": 218.93179,
            "year": 0
        }
    """
    # open song file
    # Each file contains a JSON object, representing 1 song, which is why we use lines=True
    df = pd.read_json(filepath, lines=True)

    # - The artist must be inserted first since the song has a foreign key dependency on artist.
    # - Latitude/longitude can be null, which Pandas converts into NaN
    artist_columns = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artist_data = list(df[artist_columns].values[0])
    cur.execute(sq.artist_table_insert, artist_data)

    # - There are some instances where year is 0
    song_columns = ["song_id", "title", "artist_id", "year", "duration"]
    song_data = list(df[song_columns].values[0])
    cur.execute(sq.song_table_insert, song_data)


def process_log_file(cur, filepath):
    """Processes a user active log file by insert data into users, time, and songplays tables

    A log file contains multiple lines. Each line in the log is a JSON object.

    Example log line (I've added newlines to make the data more readable):

        {
            "artist":null,
            "auth":"Logged In",
            "firstName":"Walter",
            "gender":"M",
            "itemInSession":0,
            "lastName":"Frye",
            "length":null,
            "level":"free",
            "location":"San Francisco-Oakland-Hayward, CA",
            "method":"GET",
            "page":"Home",
            "registration":1540919166796.0,
            "sessionId":38,
            "song":null,
            "status":200,
            "ts":1541105830796,
            "userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",  # noqa
            "userId":"39"
        }
    """
    # open log file
    # Each line is a JSON object, which is why we use lines=True
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    df["startTime"] = pd.to_datetime(df["ts"], unit="ms")

    # insert time data records
    time_df = pd.DataFrame({
        "start_time": df["startTime"],
        "hour": df["startTime"].dt.hour,
        "day": df["startTime"].dt.day,
        "week": df["startTime"].dt.isocalendar().week,
        "month": df["startTime"].dt.month,
        "year": df["startTime"].dt.year,
        "weekday": df["startTime"].dt.weekday,
    })
    for i, row in time_df.iterrows():
        cur.execute(sq.time_table_insert, list(row))

    user_columns = ["userId", "firstName", "lastName", "gender", "level"]
    # - Keep the last duplicate since that will be the most up to date user
    # - It seems like it's possible for a user to move from free to paid, granted
    #   it's probably not likely in the same log file.
    # - Note that dropping duplicates here does not prevent duplicates from other log files.
    #   Those duplicates will be handled by the SQL insert query which has an ON CONFLICT clause.
    #   We use drop_duplicates here to avoid inserting unnecessary duplicates.
    user_df = df[user_columns].drop_duplicates(keep="last")

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(sq.user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        # The log data does not include song_id/artist_id, so this means we have
        # to query the database to retrieve those references.
        #
        # Unfortunately it's possible that we won't find a match.
        #
        # There are a few reasons for this:
        #
        #   - One big reason is we're working with a small subset of a larger data set
        #   - Another is that the song title, artist, and length must match exactly
        cur.execute(sq.song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            # It's possible that we won't find a matching song_id/artist_id, especially
            # with the subset of data we're working with.
            #
            # In this scenario, the values will be set to NULL.
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row.startTime,
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent,
        )
        cur.execute(sq.songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Processes data given a filepath and a data processing function"""
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f"{i}/{num_files} files processed.")


def main():
    """The main entrypoint of the ETL script"""
    conn = psycopg2.connect(SPARKIFY_DB_CONNECTION_STRING)
    cur = conn.cursor()

    process_data(cur, conn, filepath=SONG_DATA_PATH, func=process_song_file)
    process_data(cur, conn, filepath=LOG_DATA_PATH, func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
