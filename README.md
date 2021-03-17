# Projects-with-Spark

_Big data with Apache Spark in python_

- [Overview](#overview)
- [Projects](#projects)
- [Credits](#credits)


## Overview

This repo consists of some useful codes that was developed as part of some activities and exercises in a course on Apache Spark that I took in Udemy. I have had prior experience dealing with all the these (except breadth-first-search and streaming data) albeit in a non-distributed context. It was really interesting to apply these methods to large datasets (even on my personal computer) using spark RDDs and dataframe framework. It was quite fascinating to get to know the breadth-first-search algorithm and apply it to find the degree of separation in a (fake) social media graph. Apart from these, I also tried my hands on dealing with the SparkML and Structured Streaming which correspond to the Machine learning and streaming data services offered by Apache Spark.   


## Projects

- Movie Recommendation : Used the 1-Million movies dataset from the [grouplens](https://grouplens.org/datasets/movielens/) website. Learned how to use Amazon's Elastic Map Reduce service (EMR) to run Spark on top of the Hadoop Cluster manager (YARN). A cluster framework becomes necessary because of the self-join operation on the massive movie dataset. However, my local machine (8-cores) was able to handle it well, so I instead distributed the Spark job on my local machine. The dataframe version of the code can be found [here](https://github.com/jyotisman-ds/Projects-with-Spark/blob/main/movie-similarities-dataframe.py) while the RDD version is [here](https://github.com/jyotisman-ds/Projects-with-Spark/blob/main/movie-similarities-1m.py).


<p align="center">
  <img src="https://github.com/jyotisman-ds/Projects-with-Spark/blob/main/bfs.png">
</p>

<p align="center">
Picture Courtesy : https://www.gatevidyalay.com/breadth-first-search-bfs-algorithm
</p>

- Breadth-first-search : Reduced the breadth-first-search algorithm to a mapReduce job and then it was down to implementing the map and reduce components individually. We applied this to a marvel superheroes dataset (connections of every superhero in the marvel universe) provided in the course. And, it was interesting to see that even the most obscure heroes were only 2 degrees of separation apart from the most popular ones. The code to obtain the obscure characters in the dataset (based on least number of connections to other heores) can be found [here](https://github.com/jyotisman-ds/Projects-with-Spark/blob/main/ObsureSuperheroes.py) and the breadth-fast-search algo. is [here](https://github.com/jyotisman-ds/Projects-with-Spark/blob/main/degrees-of-separation.py).


- SparkML - Even though there isn't a great deal of choices for ML algorithms in Spark, it still contains a decent list of some of the poppular algorithms. This also boils to which algorithms could actually be parallelized. A [code](https://github.com/jyotisman-ds/Projects-with-Spark/blob/main/Houseprices_DecisionTree.py) for using an unoptimized Decision tree regression on a house prices dataset is also present in this repo for demonstration purposes. The [dataset](https://archive.ics.uci.edu/ml/datasets/Real+estate+valuation+data+set) is obtained from the UC Irvine repository of public datasets.

- Structured Streaming - Used the new structured streaming framework for dataframes to read a curated logs data for demonstration. Also introduced a window and a slider interval to find the top URLs that were logged into in the last 30 seconds. The data used was an old logs file, so used the current_timestamp method in Spark to create an "eventTime" column to simulate the real time. The relevant code can be found [here](https://github.com/jyotisman-ds/Projects-with-Spark/blob/main/Str_Streaming_accessLogs.py).



## Credits

Thanks to Frank Kane for teaching this excellent course on [Taming Big Data with Apache Spark](https://www.udemy.com/course/taming-big-data-with-apache-spark-hands-on/) and providing interesting hands-on activities/exercises. The codes in this repo were developed from using the coding implementations provided as part of this course. Also, thanks to Udemy for hosting this wonderful content.
