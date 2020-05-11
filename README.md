# Sparkify Data Modelling

A company called Sparkify collects data on songs and customers' activity on their music streaming platform. They want to analyze this data to know what the customers are listening to using their music streaming. the song data and users' log activities are stored in JSON based files. They want to create a PostgresSQL database and perform ETL from the JSON files into the database. The goal of this project is to help the analytic team to understand what songs users are listening to. To get more insigt on thier business to improve thier platform. 

In this project, we used python, Jupyter Notebook and PostgresSQL database to achieve their goal. First, we will extract the data from JSON files and store them into data frames using Pandas. Then, start modeling the data based on the required analysis from the analysis team. After that,
we create the tables in the database using the star schema method. Finally, we loaded the data from data frames into the tables in the database.

#### Tables and dataset
Songplays table is the fact table of this dataset, which contains all the primary keys of the other dimension tables as a forigen key.
The other tables play as dimension tables, which store all the information about each entiitity and all of them have a realatuionship with the fact table songplays
##### Tables:
1. Fact table [songplays]: records in log data associated with song play (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
2. Dimension table [users]: users in the app (user_id, first_name, last_name, gender, level)
3. Dimension table [songs]: songs in music database (song_id, title, artist_id, year, duration)
4. Dimension table [artists]: artists in music database (artist_id, name, location, latitude, longitude)
5. Dimension table [time]:timestamps of records in songplays broken down into specific units (start_time, hour, day, week, month, year, weekday)

###### ERD:

![ERD](/erd.png)

#### Prerequisites
To run the project, first, you need:
1. Python 3.x installed.
2. PostgresSQL database installed.
3. Create a user named: student and password: student and Database named: studentdb (you can replace this information with whatever you want, but if you do, you have to update the function create_database() inside file create tables.py)


#### Running the project
1. Run "create_tables.py". This file will create the database "sparkifyDB" with empty tables
2. Run "etl.py" This file will load the data into the database "sparkifyDB" and display how many files have been successfully processed.