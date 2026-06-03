class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT  
            song_id,
            title, 
            artist_id, 
            year,
            duration
        FROM (
            SELECT song_id,
                title,
                artist_id,
                year,
                duration,
                ROW_NUMBER() OVER (PARTITION BY song_id ORDER BY year DESC) AS row_num
            FROM staging_songs
            WHERE song_id IS NOT NULL
        )
        WHERE row_num = 1;
    """)

    artist_table_insert = ("""
        SELECT  
            artist_id,
            name,
            location,
            latitude,
            longitude
        FROM (
            SELECT  artist_id,
                    artist_name AS name,
                    artist_location AS location,
                    artist_latitude::DECIMAL(9,6) AS latitude,
                    artist_longitude::DECIMAL(9,6) AS longitude,
                    ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY artist_name) AS row_num
            FROM staging_songs
            WHERE artist_id IS NOT NULL
        )
        WHERE row_num = 1;
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    