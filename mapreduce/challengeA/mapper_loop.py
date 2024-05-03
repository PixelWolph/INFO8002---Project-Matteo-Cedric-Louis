#!/usr/bin/env python
# -*-coding:utf-8 -*
import sys
import os
import io

def parse_string_to_number(s):
    if s == 'i':
        return float('inf')
    else:
        return int(s) 



# The point of this mapper is to allow the reducer to advance in the search as
# it emits the new frontier
def main():

    # Wrap sys.stdin and sys.stdout to enforce UTF-8
    sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    for line in sys.stdin:
        # We extract all the information for the node
        parts = line.split('\t')

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

        # We first emit this actor back but change the processed flag to 1 if the distance is finite
        if actor_distance_number == float('inf'):
            print(actor_id_cleaned + "\t" + actor_name_cleaned + "\ti" + "\t0" + "\t" + str(actor_adj_cleaned))
        else:
            print(actor_id_cleaned + "\t" + actor_name_cleaned + "\t" + str(actor_distance_number) + "\t1" + "\t" + str(actor_adj_cleaned))

        # For each adjacent node, we emit a line with d+1 ONLY IF OUR DISTANCE IS NOT INF AND IF THE NODE HASN'T BEEN PROCESSED YET
        if actor_distance_number != float('inf') and processed_number != 1:
            new_distance = actor_distance_number+1
            for actor in actor_relations:
                clean_id = actor.strip().replace('\n', '').replace('\t', '')
                print(str(clean_id) + "\t" + str(new_distance))

if __name__ == "__main__":
    main()