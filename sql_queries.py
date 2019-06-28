import configparser


# CONFIG
config = configparser.ConfigParser()

# read connection details from the config file
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES


staging_events_table_create= ("CREATE TABLE IF NOT EXISTS staging_events( \
                           artist text, \
                           auth text, \
                           firstName text, \
                           gender text, \
                           itemInSession text, \
                           lastName text, \
                           length text, \
                           level text, \
                           location text, \
                           method text, \
                           page text, \
                           registration text, \
                           sessionId text, \
                           song text,\
                           status text, \
                           ts bigint, \
                           userAgent text, \
                           userId text \
                            );")


staging_songs_table_create = ("CREATE TABLE IF NOT EXISTS staging_songs( \
                           num_songs text, \
                           artist_id text, \
                           artist_latitude text, \
                           artist_logitude text, \
                           artist_location text, \
                           artist_name text, \
                           song_id text, \
                           title text, \
                           duration text, \
                           year text \
                            );")

songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays( \
                           songplay_id int IDENTITY not null, \
                           start_time timestamp, \
                           level varchar, \
                           user_id varchar, \
                           song_id varchar, \
                           artist_id varchar, \
                           session_id int, \
                           location text, \
                           user_agent text, \
                           primary key(songplay_id), \
                           foreign key(user_id) references users(user_id), \
                           foreign key(song_id) references songs(song_id), \
                           foreign key(artist_id) references artists(artist_id), \
                           foreign key(start_time) references time(start_time)) \
                           distkey(artist_id) sortkey(start_time);")

user_table_create = ("CREATE TABLE IF NOT EXISTS users( \
                       user_id varchar not null primary key, \
                       first_name varchar, \
                       last_name varchar, \
                       gender varchar, \
                       level varchar \
                    ) diststyle all;")

song_table_create = ("CREATE TABLE IF NOT EXISTS songs( \
                       song_id varchar not null,\
                       title varchar ,\
                       artist_id varchar , \
                       year int, \
                       duration float8, \
                       primary key(song_id), \
                       foreign key(artist_id) references artists(artist_id) \
                      ) distkey(artist_id);")

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists( \
                         artist_id varchar not null, \
                         name varchar, \
                         location varchar, \
                         latitude float, \
                         longitude float, \
                         primary key(artist_id) \
                       ) distkey(artist_id);")

time_table_create = ("CREATE TABLE IF NOT EXISTS time( \
                       start_time timestamp not null, \
                       hour int, \
                       day int, \
                       week int, \
                      month int, \
                      year int, \
                      weekday int, \
                      primary key (start_time)\
                     ) diststyle all sortkey(start_time);")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events 
    from {} 
    iam_role {}
    compupdate off region 'us-west-2'
    JSON 's3://udacity-dend/log_json_path.json';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
    copy staging_songs  
    from {}
    iam_role {}
    compupdate off region 'us-west-2'
    JSON 'auto' truncatecolumns;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("insert into songplays (start_time,level,user_id,song_id,artist_id,session_id,location,user_agent)\
(select distinct (TIMESTAMP 'epoch' + s.ts/1000 * interval '1 second') AS start_time, \
s.level, \
s.userid, \
songs.song_id, \
songs.artist_id, \
s.sessionid::int, \
s.location, \
s.useragent \
from staging_events s , songs, artists \
where s.song = songs.title \
and s.length = songs.duration \
and artists.name = s.artist \
and songs.artist_id = artists.artist_id \
and s.page = 'NextSong' \
);")

user_table_insert = ("insert into users (select distinct userid,firstName,lastName,gender,level \
from staging_events where userid != '');")

song_table_insert = ("insert into songs (select distinct song_id, title,artist_id,year::int as year, \
duration::float8 as duration from staging_songs);")

artist_table_insert = ("insert into artists (select distinct \
artist_id,artist_name,artist_location,artist_latitude::float,artist_logitude::float from staging_songs);")

time_table_insert = ("insert into time (SELECT DISTINCT (TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS start_time, \
date_part(h, (TIMESTAMP 'epoch' + ts/1000 * interval '1 second'))::int as hour, \
date_part(d, (TIMESTAMP 'epoch' + ts/1000 * interval '1 second'))::int as day, \
date_part(w, (TIMESTAMP 'epoch' + ts/1000 * interval '1 second'))::int as week, \
date_part(mon, (TIMESTAMP 'epoch' + ts/1000 * interval '1 second'))::int as month, \
date_part(y, (TIMESTAMP 'epoch' + ts/1000 * interval '1 second'))::int as year, \
date_part(dow, (TIMESTAMP 'epoch' + ts/1000 * interval '1 second'))::int as weekday \
FROM staging_events );")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,user_table_create,artist_table_create,song_table_create, time_table_create,songplay_table_create,] 
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,songplay_table_drop,user_table_drop,time_table_drop, song_table_drop,artist_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [time_table_insert, user_table_insert, artist_table_insert, song_table_insert, songplay_table_insert]
