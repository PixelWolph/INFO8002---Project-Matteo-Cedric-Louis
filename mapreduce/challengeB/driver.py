#!/usr/bin/env python
# -*-coding:utf-8 -*
import subprocess
import os
import time
import subprocess

def run_first(ratings_input_path, title_input_path, output_path):
    # Command to run the Hadoop job
    cmd = [
        'hadoop', 'jar', '/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar',
        '-Dmapreduce.map.output.charset=UTF-8',
        '-Dmapreduce.reduce.output.charset=UTF-8',
        '-files', 'mapper_first.py,reducer_first.py',
        '-mapper', "python3 mapper_first.py",
        '-reducer', "python3 reducer_first.py",
        '-input', ratings_input_path, title_input_path,
        '-output', output_path
    ]
    subprocess.run(cmd, check=True)

def run_second(previous, output_path):
    # Command to run the Hadoop job
    cmd = [
        'hadoop', 'jar', '/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar',
        '-Dmapreduce.map.output.charset=UTF-8',
        '-Dmapreduce.reduce.output.charset=UTF-8',
        '-files', 'mapper_second.py,reducer_second.py',
        '-mapper', "python3 mapper_second.py",
        '-reducer', "python3 reducer_second.py",
        '-input', previous,
        '-output', output_path
    ]
    subprocess.run(cmd, check=True)

def run_third(previous, name_input_path, output_path):
    # Command to run the Hadoop job
    cmd = [
        'hadoop', 'jar', '/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar',
        '-Dmapreduce.map.output.charset=UTF-8',
        '-Dmapreduce.reduce.output.charset=UTF-8',
        '-files', 'mapper_third.py,reducer_third.py',
        '-mapper', "python3 mapper_third.py",
        '-reducer', "python3 reducer_third.py",
        '-input', previous, name_input_path,
        '-output', output_path
    ]
    subprocess.run(cmd, check=True)

def remove_hdfs_directory(directory_path):
    # Command to remove the directory from HDFS
    subprocess.run(['hadoop', 'fs', '-rm', '-r', directory_path], check=True)


def main():
    start_time = time.time()
    # We execute the first iteration to join information from both input files
    title_input_path = "data/title.principals.csv"
    name_input_path = "data/name.basics.csv"
    ratings_input_path = "data/title.ratings.csv"

    output_path = "output_0"
    run_first(ratings_input_path, title_input_path, output_path)

    # We execute the second iteration
    previous = "output_0"
    output_path = "output_1"
    run_second(previous, output_path)

    remove_hdfs_directory("output_0")

    # We execute the third iteration
    previous = "output_1"
    output_path = "output_2"
    run_third(previous, name_input_path, output_path)

    remove_hdfs_directory("output_1")

    end_time = time.time()

    print("Time taken :  %s seconds" % (end_time - start_time))


if __name__ == "__main__":
    main()