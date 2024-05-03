#!/usr/bin/env python
# -*-coding:utf-8 -*
import sys
from collections import defaultdict, deque
import io
import traceback

# This reducer will build the complete adjacency list for the actors it receives as well as initialize the distance to the central actor
def main():
        actors_adj_list = defaultdict(set)
        actors_name = defaultdict(str)
        #The nconst of the actor we are computing distances to
        centralActor = "nm0000102"

        # Wrap sys.stdin and sys.stdout to enforce UTF-8
        sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

        for line in sys.stdin:

            parts = line.split('\t')

            actor_id = parts[0]
            actor_name = parts[1]
            actor_adj = parts[2]

            actor_relations = actor_adj.split("-")

            actors_name[actor_id] = actor_name

            for relation in actor_relations:
                actors_adj_list[actor_id].add(relation)

        for actor, adjacent_actors in actors_adj_list.items():
            cleaned_adjacent_actors = [adj.strip().replace('\n', '').replace('\t', '') for adj in adjacent_actors]
            adjacency_list = "-".join(cleaned_adjacent_actors)
            clean_name = actors_name[actor].strip().replace('\n', '').replace('\t', '')
            
            # We compute the first distance, 0 if it is the central actor, 1 if immediatly connected, inf otherwise
            distance = float('inf')
            if actor == centralActor:
                distance = 0
            elif centralActor in actors_adj_list[actor]:
                distance = 1

            if distance == float('inf'):
                print(str(actor) + "\t" + str(clean_name) + "\ti" + "\t0" + "\t" + str(adjacency_list))
            elif distance == 1:
                print(str(actor) + "\t" + str(clean_name) + "\t" + str(distance) + "\t0" + "\t" + str(adjacency_list))
            else:
                print(str(actor) + "\t" + str(clean_name) + "\t" + str(distance) + "\t1" + "\t" + str(adjacency_list))

if __name__ == "__main__":
    main()