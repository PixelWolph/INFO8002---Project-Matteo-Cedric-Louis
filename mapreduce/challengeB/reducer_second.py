#!/usr/bin/env python
# -*-coding:utf-8 -*
import sys
from collections import defaultdict
import io



# This reducer will build a dictionary to ba able to give back 
# key/value pairs composed of filmID as key, actorName and actorID as value.
def main():
    actors_ratings = defaultdict(list)

    # Wrap sys.stdin and sys.stdout to enforce UTF-8
    sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    for line in sys.stdin:
        parts = line.split('\t')
        actor_id = parts[0].strip()

        average = parts[1].strip()

        number_films = int(parts[2].strip())

        if average != "na":
            average_value = float(average)
            actors_ratings[actor_id].append((average_value, number_films))

    # At this point we have a dictionary with all movies relative to an actor and
    # a dictionary with the corresponding actor ID. We just need to emit that information
    for actor_id, rating_tuples in actors_ratings.items():
        total_average = 0.0
        total_films = 0

        for tuple in rating_tuples:
            total_average += tuple[0]*tuple[1]
            total_films += tuple[1]

        final_average = total_average/total_films

        print(actor_id + "\t" + str(final_average))

if __name__ == "__main__":
    main()