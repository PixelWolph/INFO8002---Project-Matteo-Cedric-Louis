#!/usr/bin/env python
# -*-coding:utf-8 -*
import sys
import os
import io

# The point of this mapper is to allow the reducer to join 2 csv files based on actor IDs
def main():

    # Wrap sys.stdin and sys.stdout to enforce UTF-8
    sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    # Extremely easy as the previous reducer has already done the job
    for line in sys.stdin:
        # Check if the last character of the line is a line feed
        if line.endswith('\n'):
            # Print the line without adding an additional newline
            print(line, end='')
        else:
            # If the line does not end with a newline, print it and add a newline
            print(line)

if __name__ == "__main__":
    main()