import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Extract the song and artist data from the JSON file and write to the song and artist
    database tables.
    
    cur -- database cursor used to write to song and artist tables
    filepath -- JSON file containing data for song and artist
    
    The assumption is that only one song is defined in each file since only the first 
    song and artist are extracted and written to the database tables.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = (
        df[['song_id', 'title', 'artist_id', 'year', 'duration']]
        .values[0]
        .tolist()
    )
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = (
        df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
        .values[0]
        .tolist()
    )
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Extract songplay and timestamp data from the log file and write records to the
    songplay, user and time database tables corresponding to each entry in the log.
    
    cur -- database cursor used to write to songplay, user and time tables
    filepath -- JSON file containing log data for song plays
    
    All lines of the JSON data file are processed recursively but there are no data validation
    checks before insert.
    
    An attempt is made to identify the song_id and artist_id for each log entry 
    by searching the song and artist tables.  If no match is found song_id and artist_id 
    are populated with the value None.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df.page=='NextSong', :]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (df.ts, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday_name)
    column_labels = ('timestamp', 'hour', 'day', 'weekofyear', 'month', 'year', 'weekday_name')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName',  'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Traverse the directory tree rooted at filepath and apply the function to each JSON file."""
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """Connect to the database and process the song and log file data."""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
