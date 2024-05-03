import pyspark.sql.functions as F
from pyspark.sql.functions import array, lit, col, explode, when, struct
from pyspark.sql import SparkSession
import time
from pyspark.sql import DataFrame

def bfs_shortest_paths(edges, start_node, max_iterations):
    # Initialize the frontier with the start node and distance 0
    frontier = spark.createDataFrame([(start_node, 0)], ["id", "distance"])
    
    # Seen DataFrame to keep track of visited nodes and their shortest distance
    seen = frontier

    for i in range(max_iterations):
        start_time_iter = time.time()
        frontier_rdd = frontier.rdd.coalesce(1)
        frontier_rdd.saveAsTextFile("hdfs://namenode:9000/output"+str(i)+"/frontier")
        # Generate new frontier by joining with edges
        new_frontier = frontier.join(edges, frontier.id == edges.src) \
                               .select(edges.dst.alias("id"), (frontier.distance + 1).alias("distance")) \
                               .distinct()

        # Exclude already seen nodes
        new_frontier = new_frontier.join(seen, "id", "left_anti")

        # If no new nodes are visited, stop the loop
        if new_frontier.rdd.isEmpty():
            break

        # Update seen DataFrame with new nodes
        seen = seen.union(new_frontier)

        # Update the frontier
        frontier = new_frontier
        print("Elapsed time iteration " + str(i) + " : " + str(time.time() - start_time_iter) + " seconds")

    return seen

start_time = time.time()

spark = SparkSession.builder.appName("Actor distance compute").getOrCreate()

films_df = spark.read.csv("hdfs://namenode:9000/data/imdb/title.principals.tsv", sep="\t", header=True, inferSchema=True)

filtered_films_df = films_df.filter((films_df.category == "actor") | (films_df.category == "actress"))

# Create a pair of actors for each film
edges_df = filtered_films_df.alias("f1") \
    .join(filtered_films_df.alias("f2"), (F.col("f1.tconst") == F.col("f2.tconst")) & (F.col("f1.nconst") != F.col("f2.nconst"))) \
    .select(F.col("f1.nconst").alias("src"), F.col("f2.nconst").alias("dst"))

bidirectional_edges_df = edges_df.select(
    col("src").alias("src"), col("dst").alias("dst")
).union(
    edges_df.select(
        col("dst").alias("src"), col("src").alias("dst")
    )
)

# Remove duplicates in case the original data already had some bidirectional edges
bidirectional_edges_df = bidirectional_edges_df.distinct()

# Remove duplicates since any pair of actors can appear together in multiple films
edges_df = edges_df.dropDuplicates()

actor_ids = bidirectional_edges_df.select("src").union(bidirectional_edges_df.select("dst")).distinct().withColumnRenamed("src", "actor_id")

# Assuming 'specified_actor_id' is the actor ID you're interested in
central_actor_id = "nm0000102"

# Calculate shortest paths from the specified actor to all others
results_df = bfs_shortest_paths(bidirectional_edges_df, central_actor_id, 20)

# Write the formatted results to a text file
results_df.select("formatted").write.text("spark_result.txt")

print("Elapsed time : " + str(time.time() - start_time) + " seconds")