#!/usr/bin/env python
# -*-coding:utf-8 -*
import sys
from collections import defaultdict, deque
import io

# This reducer will take all information regarding some films and build partial
# adjacency list for the actors it encounters
def main():
    actors_partial_adj = defaultdict(set)
    film_actors = defaultdict(set)
    actors_name = defaultdict(str)

    # Wrap sys.stdin and sys.stdout to enforce UTF-8
    sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    for line in sys.stdin:
        parts = line.split('\t')
        key = parts[0]
        actorID = parts[1]
        actorName = parts[2]

        film_actors[key].add(actorID)
        actors_name[actorID] = actorName

    # Now we can build the partial adjacency list
    for film, actors in film_actors.items():
        # For each actor in the film, add all other actors from the same film to their adjacency list
        for actor in actors:
            # Add other actors (make sure not to add the actor to their own set)
            actors_partial_adj[actor].update(actors - {actor})

    for actor, adjacent_actors in actors_partial_adj.items():
        adjacency_list = "-".join(adjacent_actors)
        clean_name = actors_name[actor].strip().replace('\n', '').replace('\t', '')
        print(str(actor) + "\t" + str(clean_name) + "\t" + str(adjacency_list))

if __name__ == "__main__":
    main()