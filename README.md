# Udacity Project: Data Modeling with Postgres

## Purpose of the database

- Sparkify's analytics team wants to understand what songs their users are listening to
- Currently song user activity data are stored in log files formatted in JSON format
- The JSON data is not easy to query
- This database was created to make it easier for Sparkify's analytics team to find insights using SQL

## Database schema design decisions

The database design uses a star schema to allow the analytics team to use simpler queries.

The combination of fact and dimension tables should allow for many types of useful queries that
the analytics team may want to answer.

### Fact Table: songplays

There is one fact table, which is the songplays table. This table contains all user events from the
logs where a user played a song. All other events were filtered out from the logs during ingestion.

#### Schema

```sql
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
```

#### Schema Notes

- Since the songplay_id is a database incremented ID, it's possible to reinsert the same event
  if you ingest the same log files multiple times.

### Dimension Table: songs

The songs dimension can be used to ask questions, such as:

- How many users enjoyed songs from the 60's?
- How many users listened to songs less than two minutes?

#### Schema

```sql
CREATE TABLE IF NOT EXISTS songs (
    song_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    artist_id TEXT NOT NULL REFERENCES artists (artist_id),
    year INT NOT NULL,
    duration DOUBLE PRECISION NOT NULL
);
```

#### Schema Notes

- There are instances in the sample data where the year is equal to 0.
  - I'm assuming this means the year is unknown

### Dimension Table: artists

The artists dimension can be used to ask some interesting questions about
location. Granted these are harder to use since not all artists have data
for location or latitude/longitude.

In addition the location column has inconsistent formatting.

Also the latitude/longitude data would likely require some GIS functionality
to be used meaningfully.

#### Schema

```sql
CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL
);
```

#### Schema Notes

- It's possible for latitude/longitude to `NaN` (not to be confused with NULL)
- Formatting of location column is inconsistent
  - Examples:
      - Hamilton, Ohio
      - Noci (BA)
      - New York, NY [Manhattan]
      - Fort Worth, TX
      - Denmark
      - California - SF

### Dimension Table: users

The users dimension can be used to ask useful questions about
basic demographics (gender) and also account type (free/paid).

#### Schema

```sql
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    gender CHAR(1) CHECK (gender IN ('M', 'F')),
    level TEXT CHECK (level IN ('free', 'paid'))
);
```

### Dimension Table: time

The time dimension is one of the most interesting tables. Based on the
way the date/time parts are broken up, one can ask many fascinating questions,
such as:

- What time of day do most paid users listen to songs?
- How does the popularity of song change from month to month?

#### Schema

```sql
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
);
```

## Usage

### Main files

- data/log_data
  - Contains sample log data
  - Log data is partitioned by year and month
  - Example: data/log_data/2018/11/2018-11-01-events.json
- data/song_data
  - Contains sample song data
  - Song data is partitioned by the first three letters of the song's Track ID
  - Example: data/song_data/A/A/A/TRAAAAW128F429D538.json
- src/create_tables.py
  - Creates/Resets resets Sparkify database
- src/etl.py
  - Runs the ETL pipeline to process Sparkify song and activity log data into the Sparkify database
- src/example_config.py
  - Example configuration file. A config.py needs to be created to run the create_tables.py and etl.py
- requirements.txt
  - Python dependencies required to run scripts

### Dependencies

- A Postgres database must be installed
  - For development I installed an instance of Postgres 12 using Docker
- Python 3.7+
  - See requirements.txt for library dependencies

### Running the scripts

```bash
cd src;
python create_tables.py;
python etl.py;
```

#### Example output

```
80 files found in ../data/song_data
1/80 files processed.
2/80 files processed.
3/80 files processed.
4/80 files processed.
5/80 files processed.
6/80 files processed.
7/80 files processed.
8/80 files processed.
9/80 files processed.
10/80 files processed.
11/80 files processed.
12/80 files processed.
13/80 files processed.
14/80 files processed.
15/80 files processed.
16/80 files processed.
17/80 files processed.
18/80 files processed.
19/80 files processed.
20/80 files processed.
21/80 files processed.
22/80 files processed.
23/80 files processed.
24/80 files processed.
25/80 files processed.
26/80 files processed.
27/80 files processed.
28/80 files processed.
29/80 files processed.
30/80 files processed.
31/80 files processed.
32/80 files processed.
33/80 files processed.
34/80 files processed.
35/80 files processed.
36/80 files processed.
37/80 files processed.
38/80 files processed.
39/80 files processed.
40/80 files processed.
41/80 files processed.
42/80 files processed.
43/80 files processed.
44/80 files processed.
45/80 files processed.
46/80 files processed.
47/80 files processed.
48/80 files processed.
49/80 files processed.
50/80 files processed.
51/80 files processed.
52/80 files processed.
53/80 files processed.
54/80 files processed.
55/80 files processed.
56/80 files processed.
57/80 files processed.
58/80 files processed.
59/80 files processed.
60/80 files processed.
61/80 files processed.
62/80 files processed.
63/80 files processed.
64/80 files processed.
65/80 files processed.
66/80 files processed.
67/80 files processed.
68/80 files processed.
69/80 files processed.
70/80 files processed.
71/80 files processed.
72/80 files processed.
73/80 files processed.
74/80 files processed.
75/80 files processed.
76/80 files processed.
77/80 files processed.
78/80 files processed.
79/80 files processed.
80/80 files processed.
31 files found in ../data/log_data
1/31 files processed.
2/31 files processed.
3/31 files processed.
4/31 files processed.
5/31 files processed.
6/31 files processed.
7/31 files processed.
8/31 files processed.
9/31 files processed.
10/31 files processed.
11/31 files processed.
12/31 files processed.
13/31 files processed.
14/31 files processed.
15/31 files processed.
16/31 files processed.
17/31 files processed.
18/31 files processed.
19/31 files processed.
20/31 files processed.
21/31 files processed.
22/31 files processed.
23/31 files processed.
24/31 files processed.
25/31 files processed.
26/31 files processed.
27/31 files processed.
28/31 files processed.
29/31 files processed.
30/31 files processed.
31/31 files processed.
```
