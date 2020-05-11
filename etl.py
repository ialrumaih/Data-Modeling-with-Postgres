import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This function reades from the songs json files and store them into two dataframes 
    one is for song data and the other for the artists data
    
    Function Parameters:
    - cur: is a cursor class from psycopg2 library and it provides methods to execute PostgresSQL command
    - filepaht: is a string that store the file path of the JSON files 
    """
    
    # opening song file and load it into a dataframe
    df = pd.read_json(filepath, lines=True)

    # inserting the requierd data into songs table
    song_data = df[['song_id', 'title','artist_id','year','duration']].values[0]
    cur.execute(song_table_insert, song_data)
    
    # inserting the requierd data into artists table
    artist_data = df[['artist_id', 'artist_name','artist_location','artist_latitude','artist_longitude']].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This function reades from the log json files and store them into trhee dataframes 
    the first one is for time data, the second one is for user data and the last one is for songplays data
    
    Function Parameters:
    - cur: is a cursor class from psycopg2 library and it provides methods to execute PostgresSQL command
    - filepaht: is a string that store the file path of the JSON files 
    """
    
    # opening log file and load it into a dataframe
    df = pd.read_json(filepath, lines=True)
    # filtering by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # #inserting the time data and the columns into new dataframe using using dictionary and zip functions
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # inserting the requierd data into users table
    user_df = df[['userId', 'firstName','lastName','gender','level']]
    
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # inserting the requierd data into songsplay table
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None,None
        
        #Convirting the ts value into timestamp
        timestamp = pd.to_datetime(row.ts, unit='ms')
        # inserting the requierd data into songsplay table
        songplay_data = (timestamp, row.userId, row.level, songid, artistid, row.sessionId, 
    row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This function for getting all the JSON files filtring on extention .json and pront out the number of files 
    and wheater it been processed or not
    
    Function Parameters:
    - cur: is a cursor class from psycopg2 library and it provides methods to execute PostgresSQL command
    - conn: is to establish and create the connection to the databse 
    - filepaht: is a string that store the file path of the JSON files
    - func: spesify which function that we need to process 
    """

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
    """
    This is the main function, which will be execxuting first when we run this python file. 
    it will connect to the database and store it into variable that will be used in other function 
    Also, it will create a cursor and store it into a variable.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()