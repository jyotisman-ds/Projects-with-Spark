#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 14 19:23:19 2021

@author: jyotisman
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("ObscureSuperheroes").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///Users/jyotisman/Library/Mobile Documents/com~apple~CloudDocs/PySpark Course/Marvel+Names.txt")

lines = spark.read.text("file:///Users/jyotisman/Library/Mobile Documents/com~apple~CloudDocs/PySpark Course/Marvel+Graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
mostObscure = connections.filter(func.col("connections") <= 1)


# Or find the minimmum value for the connections column via
# minConnectionCount = connections.agg(func.min("connections")).first()[0]

mostObscure_joined = mostObscure.join(names, "id")

#mostObscure = mostObscure.withColumn("heroname", lookupName(func.col("id")))

#mostObscureNames = names.filter(func.col("id") == mostObscure.id).select("name")
mostObscure_final = mostObscure_joined.sort("connections").select("name", "connections")

mostObscure_final.show(mostObscure_final.count())



