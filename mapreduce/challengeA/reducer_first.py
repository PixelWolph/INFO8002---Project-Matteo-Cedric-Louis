#!/usr/bin/env python
# -*-coding:utf-8 -*
import sys
from collections import defaultdict
import io

# This reducer will build a dictionary to be able to give back 
# key/value pairs composed of filmID as key, actorName and actorID as value.
def main():
    actors_name = defaultdict(str)
    actors_movies = defaultdict(set)

    # Wrap sys.stdin and sys.stdout to enforce UTF-8
    sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    for line in sys.stdin:
        parts = line.split('\t')
        actorID = parts[0].strip()
        value = parts[1]

        if value.startswith("A_"):
            name = value.split("_")[1].strip()
            actors_name[actorID] = name
        elif value.startswith("F_"):
            tconst = value.split("_")[1].strip()
            actors_movies[tconst].add(actorID)

    # At this point we have a dictionary with all movies relative to an actor and
    # a dictionary with the corresponding actor name. We just need to emit that information
    for tconst, actorIDs in actors_movies.items():
        for actorID in actorIDs:
            # Retrieve the list of names associated with the actorID
            name = actors_name[actorID]
            clean_tconst = tconst
            clean_name = name if name else ''
            print(str(clean_tconst) + "\t" + str(actorID) + "\t" + str(clean_name))

if __name__ == "__main__":
    main()