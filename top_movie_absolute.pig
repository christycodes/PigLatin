--Identifies top movie for each year, then attach actor names--

movies = load '...data/movie/imdb.tsv' using PigStorage('\t') 
         as (actor:chararray, movie:chararray, year:int);

movie_weights = load '/Users/christyloke/hadoop/hadoop-student-homework/data/movie-weights/imdb-weights.tsv' using PigStorage('\t') 
         as (movie:chararray, year:int, weight:float);

movies_join = JOIN movies BY (year,movie), movie_weights BY (year,movie);

movies_actors = FOREACH movies_join GENERATE 
		movies::movie AS movie, 
		movies::actor AS actor, 
		movies::year AS year, 
		movie_weights::weight AS weight;

movies_group = GROUP movies_actors BY year;

top_weight = FOREACH movies_group GENERATE FLATTEN(movies_actors), MAX(movies_actors.weight) AS weight;

movies_filter = FILTER top_weight BY movies_actors::weight == weight;

movies_year_weight_group = GROUP movies_filter BY (year,weight,movie);

movies_result = FOREACH movies_year_weight_group{
  actors = DISTINCT movies_filter.movies_actors::actor;
  GENERATE
    group.movies_actors::movie, 
    group.movies_actors::year, 
    group.weight, 
    BagToString(actors,',');
}

--dump movies_result

STORE movies_result INTO '...output/top-movie-weight' USING PigStorage();
