
# MapReduce with Spark RDDs
## Overview

There are two parts in this project.

- In the first part, I have implemented a MapReduce program that gives the number of shifts that co-workers worked together in descending order. If number of shifts are same then results will be ordered by co-workers's name's alphabets.  

- In the second part, I have written the program that uses spark data-frames not the RDDs and produces the answers to the following questions. 

1. What is the name of the airline that had the most canceled flights on September 2021?
2. How many flights were diverted between the period of 20th-30th November 2021?
3. What is the average airtime for the flights from “Nashville, TN” to “Chicago, IL”?
4. How many unique days are missing departure time (DepTime)?


## A Little about RDD, MapReduce and Spark

Apache Spark is a distributed computing platform that provides high-level APIs in Java, Python, and Scala. It was designed to be faster than Hadoop MapReduce and easier to use.

One of the core data structures in Spark is the Resilient Distributed Dataset (RDD). An RDD is an immutable distributed collection of data that can be processed in parallel. RDDs are created by starting with a file or collection in your driver program and parallelizing it. You can then apply transformations to the data in the RDD and actions to compute a result.

MapReduce is a programming model for processing large data sets with a parallel, distributed algorithm on a cluster. It was inspired by the map and reduce functions in functional programming. The MapReduce model consists of two main phases: the map phase, in which the input data is split into smaller chunks and processed by map function; and the reduce phase, in which the output of the map phase is aggregated and processed by the reduce function.

In Spark, the map and reduce functions are part of the RDD API. You can use these functions to apply transformations to the data in your RDD and to express computations on the data. The map function takes a function as an argument and applies it to each element in the RDD, while the reduce function takes a function and combines the elements of the RDD using that function.

For example, you could use map to apply a function to each element in an RDD that squares the element, and then use reduce to sum the squared elements. This would give you the sum of the squares of all the elements in the RDD.

Overall, RDDs and the MapReduce model are important tools in Spark for distributed data processing and parallel computing.

## Run Locally  💫
Before running the project make sure you have python installed in your computer.

Clone the project.

```bash
  git clone https://github.com/ronakjpatel/map_reduce_spark.git
```

Go to the project directory

```bash
  cd [dir-name]
```

Download the dataset from [here](https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022?select=Combined_Flights_2021.csv). Make sure both of the data-set files are in your current working directory. 

Install the pyspark module in your python environment. 
```bash
  pip install pyspark
```

Run the script.

```bash
  python script.py
```


## Lessons Learned 💪

- Distributed data processing and parallel computing: I have learnt how to use Spark and RDDs to process large datasets in parallel across a cluster of machines.

- The MapReduce programming model: I have learnt how to use the map and reduce functions to apply transformations to data and express computations on it.

- The Spark API: I have become familiar with the various functions and transformations available in the Spark RDD API and how to use them to manipulate and process data.


## Tech Stack

Python 🐍,  
PySpark ⚡️


## 🔗 Links
[![portfolio](https://img.shields.io/badge/my_portfolio-000?style=for-the-badge&logo=ko-fi&logoColor=white)](www.patelrj.com) 

[![linkedin](https://img.shields.io/badge/linkedin-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/ronak-p/)

[![medium](https://img.shields.io/badge/Medium-12100E?style=for-the-badge&logo=medium&logoColor=white)](https://medium.com/@rjpatel7991)

