#!/usr/bin/env python
# -*-coding:utf-8 -*
import subprocess
import os
import time
import subprocess

def run_first(name_input_path, title_input_path, output_path):
    # Command to run the Hadoop job
    cmd = [
        'hadoop', 'jar', '/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar',
        '-Dmapreduce.map.output.charset=UTF-8',
        '-Dmapreduce.reduce.output.charset=UTF-8',
        '-files', 'mapper_first.py,reducer_first.py',
        '-mapper', "python3 mapper_first.py",
        '-reducer', "python3 reducer_first.py",
        '-input', title_input_path, name_input_path,
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

def run_third(previous, output_path):
    # Command to run the Hadoop job
    cmd = [
        'hadoop', 'jar', '/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar',
        '-Dmapreduce.map.output.charset=UTF-8',
        '-Dmapreduce.reduce.output.charset=UTF-8',
        '-files', 'mapper_third.py,reducer_third.py',
        '-mapper', "python3 mapper_third.py",
        '-reducer', "python3 reducer_third.py",
        '-input', previous,
        '-output', output_path
    ]
    subprocess.run(cmd, check=True)

def run_job(previous, output_path):
    # Command to run the Hadoop job
    cmd = [
        'hadoop', 'jar', '/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar',
        '-Dmapreduce.map.output.charset=UTF-8',
        '-Dmapreduce.reduce.output.charset=UTF-8',
        '-files', 'mapper_loop.py,reducer_loop.py',
        '-mapper', "python3 mapper_loop.py",
        '-reducer', "python3 reducer_loop.py",
        '-input', previous,
        '-output', output_path
    ]
    subprocess.run(cmd, check=True)

def run_last(previous, output_path):
    # Command to run the Hadoop job
    cmd = [
        'hadoop', 'jar', '/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar',
        '-Dmapreduce.map.output.charset=UTF-8',
        '-Dmapreduce.reduce.output.charset=UTF-8',
        '-files', 'mapper_last.py,reducer_last.py',
        '-mapper', "python3 mapper_last.py",
        '-reducer', "python3 reducer_last.py",
        '-input', previous,
        '-output', output_path
    ]
    subprocess.run(cmd, check=True)

def parse_string_to_number(s):
    if s == 'i':
        return float('inf')
    else:
        return int(s) 

# To check the convergence, we just look if there are candidates for new iterations
# i.e. if there are nodes with finite distance that have not been processed yet
def check_convergence(output_path):
    print("Checking convergence...")
    file = output_path + "/part-00000"
    # Fetch contents of the previous output file
    content = subprocess.check_output(['hadoop', 'fs', '-cat', file])

    # Decode the binary content to a string
    text = content.decode('utf-8')
    
    converged = True

    # Iterate over each line in the decoded content
    for line in text.splitlines():
        parts = line.split('\t')

        actor_distance = parts[2]
        actor_distance_number = parse_string_to_number(actor_distance)

        processed = parts[3]
        processed_number = parse_string_to_number(processed)

        if actor_distance_number != float("inf") and processed_number == 0:
            converged = False
    
    return converged

def remove_hdfs_directory(directory_path):
    # Command to remove the directory from HDFS
    subprocess.run(['hadoop', 'fs', '-rm', '-r', directory_path], check=True)


def main():
    start_time = time.time()
    # We execute the first iteration to join information from both input files
    title_input_path = "/data/imdb/title.principals.csv"
    name_input_path = "/data/imdb/name.basics.csv"
    output_path = "/output_0"
    run_first(name_input_path, title_input_path, output_path)

    # We execute the second iteration
    previous = "/output_0"
    output_path = "/output_1"
    run_second(previous, output_path)

    remove_hdfs_directory("/output_0")

    # We execute the third iteration
    previous = "/output_1"
    output_path = "/output_2"
    run_third(previous, output_path)

    remove_hdfs_directory("/output_1")

    # We execute the looping until no further changes are seen
    iteration = 3
    converged = False
    base_output_path = "/output_"
    
    while not converged:
        previous_output_path = base_output_path + str(iteration - 1)
        current_output_path = base_output_path + str(iteration)
        
        # Run the job for the current iteration
        run_job(previous_output_path, current_output_path)

        remove_hdfs_directory(previous_output_path)
        
        # Check if the output has converged
        converged = check_convergence(current_output_path)
        
        # If not converged, prepare for the next iteration
        if not converged:
            print("Convergence not reached, iteration : " + str(iteration-2))
            iteration += 1

    print("Convergence reached, iteration : " + str(iteration-2))

    # Now we simply transform the data into a more readable format
    previous_output_path = base_output_path + str(iteration)
    current_output_path = base_output_path + str(iteration + 1)

    run_last(previous_output_path, current_output_path)

    remove_hdfs_directory(previous_output_path)

    end_time = time.time()

    print("Time taken :  %s seconds" % (end_time - start_time))


if __name__ == "__main__":
    main()