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

# The point of this reducer is simply to put the data in a more readable format as well as getting rid of unnecessary data
def main():

    # Wrap sys.stdin and sys.stdout to enforce UTF-8
    sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    for line in sys.stdin:
        # We extract all the information
        parts = line.split('\t')

        actor_id = parts[0]
        actor_id_cleaned = actor_id.replace('\n', '').replace('\t', '')

        actor_name = parts[1]
        actor_name_cleaned = actor_name.replace('\n', '').replace('\t', '')

        actor_distance = parts[2]
        actor_distance_number = parse_string_to_number(actor_distance)

        if actor_distance_number == float('inf'):
            print("Actor ID : " + actor_id_cleaned + "\tActor name : " + actor_name_cleaned + "\t Distance : infinite")
        else:
            print("Actor ID : " + actor_id_cleaned + "\tActor Name : " + actor_name_cleaned + "\tDistance : " + str(actor_distance_number))

if __name__ == "__main__":
    main()