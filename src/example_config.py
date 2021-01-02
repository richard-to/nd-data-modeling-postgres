# This is an example config file. In order to run the scripts, create a config.py
# file in the src directory.
#
# Then populate that file with the your database connection strings.


# DB connection strings
# ---------------------

# The create_tables.py file connects to a database so it can drop/create the sparkifydb.
# Make sure this Postgres user/role has the right privileges.
DEFAULT_DB_CONNECTION_STRING = "postgresql://user:password@host/defaultdb"

# This will be be the destination database for our ETL pipeline.
SPARKIFY_DB_CONNECTION_STRING = "postgresql://user:password@host/sparkifydb"


# Data paths
# ----------
LOG_DATA_PATH = "data/log_data"
SONG_DATA_PATH = "data/song_data"
