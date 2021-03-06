--counts the number of movies each actor has been in--

movies= load '.../data/movie/imdb.tsv' using PigStorage('\t') 
         as (actor:chararray, movie:chararray, year:int);

movies = FOREACH movies GENERATE actor, movie;
actor_group = GROUP movies by actor;
movies_count = FOREACH actor_group generate group AS actor, COUNT(movies) AS count;
actor_sorted = ORDER movies_count BY count DESC;

--dump actor_sorted

STORE actor_sorted INTO '.../output/movies-per-actor' USING PigStorage();
