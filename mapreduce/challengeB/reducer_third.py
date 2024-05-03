#!/usr/bin/env python
# -*-coding:utf-8 -*
import sys
from collections import defaultdict
import io

# This reducer will build a dictionary to ba able to give back 
# key/value pairs composed of filmID as key, actorName and actorID as value.
def main():
    actors_ratings = defaultdict(float)
    actors_names = defaultdict(str)

    # Wrap sys.stdin and sys.stdout to enforce UTF-8
    sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    for line in sys.stdin:
        parts = line.split('\t')
        actor_id = parts[0].strip()
        value = parts[1]

        if value.startswith("A_"):
            actor_name = value.split("_")[1].strip()
            actors_names[actor_id] = actor_name
        else:
            actor_rating = value.strip()
            if actor_rating != "na":
                actors_ratings[actor_id] = float(actor_rating)
            else:
                actors_ratings[actor_id] = -1.0

    # At this point we have a dictionary with all movies relative to an actor and
    # a dictionary with the corresponding actor ID. We just need to emit that information
    for actor_id, actor_rating in actors_ratings.items():
        actor_name = actors_names[actor_id]
        if actor_rating != -1:
            print("Actor ID : " + actor_id + "\tActor name : " + actor_name + "\tAverage rating : " + str(actor_rating))
        else:
            print("Actor ID : " + actor_id + "\tActor name : " + actor_name + "\tAverage rating : na")

if __name__ == "__main__":
    main()