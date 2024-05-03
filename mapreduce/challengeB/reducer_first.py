#!/usr/bin/env python
# -*-coding:utf-8 -*
import sys
from collections import defaultdict
import io

# This reducer will build a dictionary to ba able to give back 
# key/value pairs composed of filmID as key, actorName and actorID as value.
def main():
    movies_rating = defaultdict(float)
    actors_movies = defaultdict(set)

    # Wrap sys.stdin and sys.stdout to enforce UTF-8
    sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    for line in sys.stdin:
        parts = line.split('\t')
        film_id = parts[0].strip()
        value = parts[1]

        if value.startswith("R_"):
            rating = value.split("_")[1].strip()
            rating_number = float(rating)
            movies_rating[film_id] = rating_number
        elif value.startswith("A_"):
            actor_id = value.split("_")[1].strip()
            actors_movies[actor_id].add(film_id)

    # At this point we have a dictionary with all movies relative to an actor and
    # a dictionary with the corresponding actor ID. We just need to emit that information
    for actor_id, film_ids in actors_movies.items():
        total_films = 0
        total_rating = 0.0
        for film_id in film_ids:
            if film_id in movies_rating:
                rating = movies_rating[film_id]
                total_rating += rating
                total_films += 1

        if total_films != 0:
            average = total_rating/total_films
            average_rating = str(average)

            print(actor_id + "\t" + average_rating + "\t" + str(total_films))

if __name__ == "__main__":
    main()