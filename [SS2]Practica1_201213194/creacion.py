DROP_TABLES=('DROP TABLE if exists tmp;')

TEMPORAL_CREATION=('CREATE TABLE tmp(artist VARCHAR(50),song VARCHAR(500),duration_ms int,explicit varchar(150),'
    'anio int,popularity decimal(10,4),danceability decimal(10,4),energy decimal(10,4),llave int,' 
    'loudness decimal(10,4),modo decimal(10,4),speechiness decimal(10,4),acousticness decimal(12,9),'
    'instrumentalness decimal(12,9),liveness decimal(10,4),valence decimal(10,4),tempo decimal(10,4),'
    'genre varchar(150) )')
