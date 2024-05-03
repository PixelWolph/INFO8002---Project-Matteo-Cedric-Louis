from pyspark.sql import SparkSession
from pyspark.sql import Row
import time

def main():
    start_time = time.time()

    # Create a SparkSession
    spark = SparkSession.builder.appName("ChallengeB").getOrCreate()

    # Input principals and ratings and name basics
    title_input = spark.read.csv("hdfs://namenode:9000/data/imdb/title.principals.tsv", sep="\t", header=True)
    ratings_input = spark.read.csv("hdfs://namenode:9000/data/imdb/title.ratings.tsv", sep="\t", header=True)
    name_input = spark.read.csv("hdfs://namenode:9000/data/imdb/name.basics.tsv", sep="\t", header=True)

    # replace with None
    title_input = title_input.replace("\\N", None)
    ratings_input = ratings_input.replace("\\N", None)
    name_input = name_input.replace("\\N", None)


    title_output1 = title_input.rdd.map(lambda line: ((str(line.tconst.strip()),  str(line.nconst.strip()))) if line.category in ["actor", "actress"] else None)
    ratings_output1 = ratings_input.rdd.map(lambda line: ((str(line.tconst.strip()), float(line.averageRating))))

    # remove the None
    title_output1 = title_output1.filter(lambda x: x is not None)
    ratings_output1 = ratings_output1.filter(lambda x: x is not None)

    combined = title_output1.join(ratings_output1)

    combined = combined.map(lambda x: (x[1][0], (x[0], x[1][1])))
    combined = combined.groupByKey().mapValues(list)

    actor_avg_rating_num_films_rdd = combined.mapValues(average)

    # select actor id and name
    name_first = lambda line: ((str(line.nconst.strip()),  line.primaryName.strip())) if line.primaryProfession is not None and ("actor" in line.primaryProfession or "actress" in line.primaryProfession) and (line.knownForTitles is not None) else None
    name_input = name_input.rdd.map(name_first)
    name_input = name_input.filter(lambda x: x is not None)

    name_input_df = name_input.toDF(['nconst', 'name'])
    actor_avg_rating_num_films_df = actor_avg_rating_num_films_rdd.toDF(["nconst", "ratings"])
    # join
    final = actor_avg_rating_num_films_df.join(name_input_df, on="nconst" , how="inner").select("nconst", "name", "ratings")

    final_rdd = final.rdd.coalesce(1)
    final_rdd.saveAsTextFile("hdfs://namenode:9000/output/average_ratings")

    # Stop SparkSession
    spark.stop()

    end_time = time.time()

    print("Time taken :  %s seconds" % (end_time - start_time))

def average(line):

    average_rating = 0
    films = 0

    for tconst, rating in line:
        average_rating = average_rating + rating
        films = films + 1 

    return average_rating/films, films

if __name__ == "__main__":
    main()
