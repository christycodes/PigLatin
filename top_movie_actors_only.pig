--Joins two files, and filter out top movie by year with actor names--

movie_weights = load '...data/movie-weights/imdb-weights.tsv' using PigStorage('\t') 
         as (movie:chararray, year:int, weight:float);

movie_weights = FOREACH movie_weights GENERATE movie, year, weight;
movie_year = GROUP movie_weights BY year;

top_weights = FOREACH movie_year{
	order_weight = ORDER movie_weights by weight DESC;
	top_movie = LIMIT order_weight 1;
	GENERATE FLATTEN(top_movie);
}

movies = load '/Users/christyloke/hadoop/hadoop-student-homework/data/movie/imdb.tsv' using PigStorage('\t') 
         as (actor:chararray, movie:chararray, year:int);

movie_weights_join = JOIN top_weights BY (movie, year) LEFT OUTER, movies BY (movie,year);                    

movie_weights_actor = FOREACH movie_weights_join GENERATE
	top_weights::top_movie::movie AS movie,
	top_weights::top_movie::year AS year,
	top_weights::top_movie::weight AS weight,
	movies::actor AS actor;

movie_weights_actor_group = GROUP movie_weights_actor BY (movie, year, weight);

movie_weights_with_actor = FOREACH movie_weights_actor_group GENERATE
                        group.movie, group.year, group.weight, movie_weights_actor.actor;

--dump movie_weights_with_actor

STORE movie_weights_with_actor INTO '...output/top-movie-weight(2)' USING PigStorage();
          
