#!/usr/bin/env python
# -*-coding:utf-8 -*
import sys
import os
import io

# The point of this mapper is to allow the reducer to join 2 csv files based on actor IDs
def main():
    # Hadoop sets this environment variable for us
    current_file = os.environ['map_input_file']

    # Wrap sys.stdin and sys.stdout to enforce UTF-8
    sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    
    for line in sys.stdin:

        parts = line.split('\t')

        # Process lines from title.basics.csv
        if 'title.ratings.csv' in current_file:
            # We skip the header line
            if "tconst" not in parts[0]:
                film_id = parts[0].strip()
                rating = parts[1].strip()
                print(film_id + "\tR_" + rating)
        # Process lines from name.basics.csv
        elif 'title.principals.csv' in current_file:
            # We skip the header line
            if "tconst" not in parts[0]:
                # We only emit if the role is actor or actress
                if parts[3] == "actor" or parts[3] == "actress":
                    film_id = parts[0].strip()
                    actor_id = parts[2].strip()
                    print(film_id + "\tA_" + actor_id)

if __name__ == "__main__":
    main()