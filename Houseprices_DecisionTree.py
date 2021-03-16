#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 15 21:23:56 2021

@author: jyotisman
"""

from pyspark.ml.regression import DecisionTreeRegressor

from pyspark.sql import SparkSession
# from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

if __name__ == "__main__":

    spark = SparkSession.builder.appName("DecisionTree").getOrCreate()
    
    real_estate = spark.read \
      .option("header", "true") \
      .option("inferSchema", "true") \
      .csv("file:///Users/jyotisman/Library/Mobile Documents/com~apple~CloudDocs/PySpark Course/realestate.csv")
      
    columns_to_keep = ["HouseAge", "DistanceToMRT", "NumberConvenienceStores"]
      
    assembler = VectorAssembler().setInputCols(columns_to_keep).setOutputCol("features")
    
    df = assembler.transform(real_estate).select("PriceOfUnitArea", "features")
    
    # Let's split our data into training data and testing data
    trainTest = df.randomSplit([0.7, 0.3])
    trainingDF = trainTest[0]
    testDF = trainTest[1]
    
    # Now create our DecisionTree regression model
    DTreg = DecisionTreeRegressor().setFeaturesCol("features").setLabelCol("PriceOfUnitArea")
    
    # Train the model using our training data
    model = DTreg.fit(trainingDF)
    
    # Now see if we can predict values in our test data.
    # Generate predictions using our DecisionTree regression model for all features in our
    # test dataframe:
    fullPredictions = model.transform(testDF).cache()

    # Extract the predictions and the "known" correct labels.
    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("PriceOfUnitArea").rdd.map(lambda x: x[0])

    # Zip them together
    predictionAndLabel = predictions.zip(labels).collect()

    # Print out the predicted and actual values for each point
    for prediction in predictionAndLabel:
      print(prediction)


    # Stop the session
    spark.stop()