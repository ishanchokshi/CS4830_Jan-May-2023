import sys
import pyspark

if len(sys.argv) != 3:
  raise Exception("Exactly 2 arguments are required: <inputUri> <outputUri>")

inputUri=sys.argv[1]
outputUri=sys.argv[2]

sc = pyspark.SparkContext()


# Reading the input file as text file
timestamps = sc.textFile(sys.argv[1])

#Storing the first line of the file as header and filtering it out from the RDD
header = timestamps.first()
timestamps = timestamps.filter(lambda line: line != header)

#Defining a function to categorize the timestamps into 4-hour time intervals
def time_count(data):
    # Splitting the  individual lines to extract only the hour from each of them
    hours = int(data.split(',')[1][0:2])
    if 0 <= hours < 6:
        return '0-6'
    elif 6 <= hours < 12:
        return '6-12'
    elif 12 <= hours < 18:
        return '12-18'
    elif 18 <= hours < 24:
        return '18-24'

#Applying the time_count function to each element of the RDD to categorize them into time intervals
time = timestamps.map(time_count)
time_counts = time.map(lambda frame: (frame,1)) #Mapping each time interval to a count of 1
# Reducing the RDD to calculate the total count of timestamps for each time interval and saving it as a text file
time_counts_2 = time_counts.reduceByKey(lambda count1, count2: count1 + count2)
time_counts_2.saveAsTextFile(sys.argv[2])
