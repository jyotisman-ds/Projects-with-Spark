#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 15 17:11:37 2021

@author: jyotisman
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import sys

def loadMovieNames():
    movieNames = {}
    with open("/Users/jyotisman/Library/Mobile Documents/com~apple~CloudDocs/PySpark Course/ml-1m/movies.dat", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split("::")
            movieNames[int(fields[0])] = fields[1]
    return movieNames


def computeSimilarity(spark, data, method = "cosine"):
    # Compute xx, xy and yy columns
    
    if method == "pearson" :
        # pairScores = data.filter((func.col("rating1") > 2) | (func.col("rating2") > 2))
            
        pairScores = data \
          .withColumn("xx", func.col("rating1") * func.col("rating1")) \
          .withColumn("yy", func.col("rating2") * func.col("rating2")) \
          .withColumn("xy", func.col("rating1") * func.col("rating2"))
          
        calculateSimilarity = pairScores \
          .groupBy("movie1", "movie2") \
          .agg( \
            func.avg(func.col("rating1")).alias("mean1"), \
            func.avg(func.col("rating2")).alias("mean2"), \
            func.avg(func.col("xy")).alias("meanxy"), \
            func.avg(func.col("xx")).alias("meanxx"), \
            func.avg(func.col("yy")).alias("meanyy"), \
            func.count(func.col("xy")).alias("numPairs")
            )
             
        #calculateSimilarity.show()
        
        calculateSimilarity2 = calculateSimilarity \
            .withColumn("numerator", func.col("meanxy") - func.col("mean1") * func.col("mean2")) \
            .withColumn("denominator", func.sqrt((func.col("meanxx") - func.col("mean1") * func.col("mean1")) * (func.col("meanyy") - func.col("mean2") * func.col("mean2"))))
              
        # calculateSimilarity2.show()

        
        result = calculateSimilarity2 \
            .withColumn("score", \
                        func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")) \
                            .otherwise(0) \
                                ).select("movie1", "movie2", "score", "numPairs")
                      
              
    else :
        pairScores = data \
          .withColumn("xx", func.col("rating1") * func.col("rating1")) \
          .withColumn("yy", func.col("rating2") * func.col("rating2")) \
          .withColumn("xy", func.col("rating1") * func.col("rating2"))
          
        calculateSimilarity = pairScores \
          .groupBy("movie1", "movie2") \
          .agg( \
            func.sum(func.col("xy")).alias("numerator"), \
            (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"), \
            func.count(func.col("xy")).alias("numPairs")
          )
    
        result = calculateSimilarity \
            .withColumn("score", \
                        func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")) \
                            .otherwise(0) \
                                ).select("movie1", "movie2", "score", "numPairs")


    # Calculate score and select only needed columns (movie1, movie2, score, numPairs)
    

    return result

spark = SparkSession.builder.appName("MovieSimilarities1M").master("local[*]").getOrCreate()

print("\nLoading movie names...")
nameDict = loadMovieNames()


moviesSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])
    
movies = spark.read \
      .option("sep", "::") \
      .schema(moviesSchema) \
      .csv("file:///Users/jyotisman/Library/Mobile Documents/com~apple~CloudDocs/PySpark Course/ml-1m/ratings.dat")
      
      
ratings = movies.select("userId", "movieId", "rating")

moviePairs = ratings.alias("ratings1") \
      .join(ratings.alias("ratings2"), (func.col("ratings1.userId") == func.col("ratings2.userId")) \
            & (func.col("ratings1.movieId") < func.col("ratings2.movieId"))) \
      .select(func.col("ratings1.movieId").alias("movie1"), \
        func.col("ratings2.movieId").alias("movie2"), \
        func.col("ratings1.rating").alias("rating1"), \
        func.col("ratings2.rating").alias("rating2"))
          

moviePairSimilarities = computeSimilarity(spark, moviePairs).persist()


if (len(sys.argv) > 1):
    scoreThreshold = 0.97
    coOccurrenceThreshold = 1000

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter( \
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) & \
          (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurrenceThreshold))
        

    # Sort by quality score.
    results = filteredResults.sort(func.col("score").desc()).take(10)
    
    print ("Top 10 similar movies for " + nameDict[movieID])
    
    for result in results:
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
          similarMovieID = result.movie2
        
        print(nameDict[similarMovieID] + "\tscore: " \
              + str(result.score) + "\tstrength: " + str(result.numPairs))
        
spark.stop()