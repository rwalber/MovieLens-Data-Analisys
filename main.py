# Author: Walber Conceicao de Jesus Rocha
# University: Universidade Federal do Reconcavo da Bahia - UFRB

# Open file and store in dictionary the movies name. The dictionary key is the movie ID
def movie_names(path):
    movie_names = {}
    with open(path, encoding = 'ISO-8859-1') as file:
        for f in file:
            data = f.split('::')
            key = int(data[0])
            movie_names[key] = data[1]
    return movie_names

# Task1 is responsible for creating a tuple, where the entry is: movie_ID::movie_Name::movie_genres
def task1(movie):
    movie_tuple = (int(movie.split('::')[1]), (int(movie.split('::')[2]), 1))
    return movie_tuple #Output: (id_movie, (rating_movie, 1))

# Task2 is responsible for counting user ratings and vote count
def task2(movieA, movieB):
    sum_task = (movieA[0] + movieB[0])
    count = (movieA[1] + movieB[1])
    movie_tuple = (sum_task, count)
    return movie_tuple #Output: (id_movie, (sum(rating_movie), sum(count)))

# Task3 is responsible for creating a new tuple, with the average_rating_movie and id_movie
def task3(movie):
    id_movie = movie[0]
    average_rating_movie = movie[1][0]/movie[1][1]
    movie_tuple = (round(average_rating_movie, 1), id_movie)
    return movie_tuple #Output: (average_rating_movie, id_movie)

# Dictionary of movies name
movies = movie_names('ml-1m/movies.dat')

# Movies rating data
movies_rating_data = sc.textFile('ml-1m/ratings.dat')

initial_tuple_movies = movies_rating_data.map(task1)

rating_movies = initial_tuple_movies.reduceByKey(task2)

finale_movies = rating_movies.map(task3)

sorted_movies = finale_movies.sortByKey(False)

results = sorted_movies.take(100)

for result in results:
    key = result[1]
    value = result[0]
    print(movies[key]+', '+str(value))