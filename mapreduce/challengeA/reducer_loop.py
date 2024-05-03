#!/usr/bin/env python
# -*-coding:utf-8 -*
import sys
from collections import defaultdict, deque
import io

def parse_string_to_number(s):
    if s == 'i':
        return float('inf')
    else:
        return int(s) 

# This reducer will update distance value for actors when it receives new information
def main():
        actors_adj_list = defaultdict(set)
        actors_name = defaultdict(str)
        actors_distance = defaultdict(lambda: float('inf'))
        actors_processed = defaultdict(int)

        # Wrap sys.stdin and sys.stdout to enforce UTF-8
        sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

        for line in sys.stdin:

            parts = line.split('\t')

            # Case where we get info on distances
            if len(parts) == 2:
                actor_id = parts[0]

                distance = parts[1]
                distance_number = parse_string_to_number(distance.replace('\n', ''))

                # we check to see if the current distance we have is bigger
                if actors_distance[actor_id] > distance_number:
                    actors_distance[actor_id] = distance_number
            # Case for normal actor info
            else:
                actor_id = parts[0]
                actor_id_cleaned = actor_id.replace('\n', '').replace('\t', '')

                actor_name = parts[1]
                actor_name_cleaned = actor_name.replace('\n', '').replace('\t', '')

                actor_distance = parts[2]
                actor_distance_number = parse_string_to_number(actor_distance)

                processed = parts[3]
                processed_number = parse_string_to_number(processed)
            
                actor_adj = parts[4]
                actor_adj_cleaned = actor_adj.replace('\n', '').replace('\t', '')
                actor_relations = actor_adj_cleaned.split("-")

                actors_name[actor_id_cleaned] = actor_name_cleaned

                if actors_distance[actor_id_cleaned] > actor_distance_number:
                    actors_distance[actor_id_cleaned] = actor_distance_number
                
                actors_processed[actor_id_cleaned] = processed_number

                for relation in actor_relations:
                    actors_adj_list[actor_id_cleaned].add(relation)

        for actor, adjacent_actors in actors_adj_list.items():
            cleaned_adjacent_actors = [adj.strip().replace('\n', '').replace('\t', '') for adj in adjacent_actors]
            adjacency_list = "-".join(cleaned_adjacent_actors)
            clean_name = actors_name[actor].strip().replace('\n', '').replace('\t', '')

            # We get the distance
            distance = actors_distance[actor]

            if distance == float('inf'):
                print(str(actor) + "\t" + str(clean_name) + "\ti" + "\t" + str(actors_processed[actor]) + "\t" + str(adjacency_list))
            else:
                print(str(actor) + "\t" + str(clean_name) + "\t" + str(distance) + "\t" + str(actors_processed[actor]) + "\t" + str(adjacency_list))

if __name__ == "__main__":
    main()