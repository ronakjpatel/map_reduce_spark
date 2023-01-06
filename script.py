import os
from itertools import permutations

from pyspark import RDD, SparkContext
from pyspark.sql import DataFrame, SparkSession


def restaurant_shift_coworkers(worker_shifts: RDD) -> RDD:
    """
    Takes an RDD that represents the contents of the worker_shifts.txt. Performs a series of MapReduce operations via
    PySpark to calculate the number of shifts worked together by each pair of co-workers. Returns the results as an RDD
    sorted by the number of shifts worked together THEN by the names of co-workers in a DESCENDING order.
    :param worker_shifts: RDD object of the contents of worker_shifts.txt.
    :return: RDD of pairs of co-workers and the number of shifts they worked together sorted in a DESCENDING order by
             the number of shifts then by the names of co-workers.
             Example output: [(('Shreya Chmela', 'Fabian Henderson'), 3),
                              (('Fabian Henderson', 'Shreya Chmela'), 3),
                              (('Shreya Chmela', 'Leila Jager'), 2),
                              (('Leila Jager', 'Shreya Chmela'), 2)]
    """
   ## raise NotImplementedError('Your Implementation Here.')

    #In the first MapReduce run 
    # We want to map all names to all dates so 
    # Map function input would be <Date,Name> Output should be <Date [Name]>
    finished_mapped_round_1= worker_shifts.map(lambda x:(x.split(",")[1],[x.split(",")[0]]))

    # Reduce function input would be <Date,[Name]> Output should be <Date [Name]>
    #Reduce function will take in the mapped values and shuffles it and put it into the same buckets
    #So that will give me the <Date,[list of names who worked on that date]> names are appended by +.
    finished_reduced_round_1 = finished_mapped_round_1.reduceByKey(lambda x,y : x+y)

    #MapReduce Round 2
    #Mapping 
    #Now in this round we have <Date,[list of names who worked on that date]> as input. Now first I have 
    # extracted x[1] means list of names and I generate the all permutations of the pairs. because the list 
    # of names should contain more than 2 names.
    finished_mapped_round_2 = finished_reduced_round_1.map(lambda x:x[1]).flatMap(lambda x: permutations(x,2))
    #Appening one to every pair of the list
    finished_mapped_round_2 = finished_mapped_round_2.map(lambda x:(x,1))
    #Adding the numbers and sorting it
    finished_reduced_round_2 = finished_mapped_round_2.reduceByKey(lambda x,y: x+y).sortBy(lambda x:(x[1],x[0]), ascending=False)
    #returining the results 
    return finished_reduced_round_2


def air_flights_most_canceled_flights(flights: DataFrame) -> str:
    """
    Takes the flight data as a DataFrame and finds the airline that had the most canceled flights on Sep. 2021
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The name of the airline with most canceled flights on Sep. 2021.
    """
    #raise NotImplementedError('Your Implementation Here.')
    ans = flights.filter((flights.FlightDate >= '2021-09-01') & (flights.FlightDate < '2021-10-01') & (flights.Cancelled == True))
    #getting the counts and returning it
    return ans.groupBy('Airline').count().collect()[13][0]
    


def air_flights_diverted_flights(flights: DataFrame) -> int:
    """
    Takes the flight data as a DataFrame and calculates the number of flights that were diverted in the period of 
    20-30 Nov. 2021.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The number of diverted flights between 20-30 Nov. 2021.
    """
    #retrieving the rows based on query
    return flights.filter((flights.FlightDate >= '2021-11-20') & (flights.FlightDate <= '2021-11-30') & (flights.Diverted == True)).count()
   
    


def air_flights_avg_airtime(flights: DataFrame) -> float:
    """
    Takes the flight data as a DataFrame and calculates the average airtime of the flights from Nashville, TN to 
    Chicago, IL.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The average airtime average airtime of the flights from Nashville, TN to 
    Chicago, IL.
    """
    temp_ans = flights.filter((flights.OriginCityName == 'Nashville, TN') & (flights.DestCityName == 'Chicago, IL'))
    #grouping it by airtime and extracting the data frame returing it 
    return temp_ans.groupBy().avg('AirTime').collect()[0][0]



def air_flights_missing_departure_time(flights: DataFrame) -> int:
    """
    Takes the flight data as a DataFrame and find the number of unique dates where the departure time (DepTime) is 
    missing.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: the number of unique dates where DepTime is missing. 
    """
    #retrieving the data where row values are missing
    ans = flights.filter(flights.DepTime.isNull())
    #getting the counts
    ans1 = ans.groupBy('FlightDate').count()
    ans1 = ans1.select('FlightDate').count()
    #returning the answer
    return ans1 



def main():
    # initialize SparkContext and SparkSession
    sc = SparkContext('local[*]')
    spark = SparkSession.builder.getOrCreate()

    print('########################## Problem 1 ########################')
    # problem 1: restaurant shift coworkers with Spark and MapReduce
    # read the file
    worker_shifts = sc.textFile('worker_shifts.txt')
    sorted_num_coworking_shifts = restaurant_shift_coworkers(worker_shifts)
    # print the most, least, and average number of shifts together
    sorted_num_coworking_shifts.persist()
    print('Co-Workers with most shifts together:', sorted_num_coworking_shifts.first())
    print('Co-Workers with least shifts together:', sorted_num_coworking_shifts.sortBy(lambda x: (x[1], x[0])).first())
    print('Avg. No. of Shared Shifts:',
          sorted_num_coworking_shifts.map(lambda x: x[1]).reduce(lambda x,y: x+y)/sorted_num_coworking_shifts.count())
    
    print('########################## Problem 2 ########################')
    # problem 2: PySpark DataFrame operations
    # read the file
    flights = spark.read.csv('/Users/ronakpatel/Concordia_Assignments/Sem1/DSD/Assignment_2/Combined_Flights_2021.csv', header=True, inferSchema=True)

    #flights.show(5)
    #flights.printSchema()
    # ll=flights.filter(flights("Origin") == "SGU").show(False)
    # ll.show(2)
    print('Q1:', air_flights_most_canceled_flights(flights), 'had the most canceled flights in September 2021.')
    print('Q2:', air_flights_diverted_flights(flights), 'flights were diverted between the period of 20th-30th '
                                                       'November 2021.')
    print('Q3:', air_flights_avg_airtime(flights), 'is the average airtime for flights that were flying from '
                                                   'Nashville to Chicago.')
    print('Q4:', air_flights_missing_departure_time(flights), 'unique dates where departure time (DepTime) was '
                                                              'not recorded.')
    #

if __name__ == '__main__':
    main()
