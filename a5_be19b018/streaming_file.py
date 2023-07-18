from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import time

kafka_topic_name = "test-topic"
kafka_bootstrap_servers = 'localhost:9092'

if __name__ == "__main__":
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from test-topic
    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of orders_df: ")
    orders_df.printSchema()

    # Select the value column and the timestamp column and cast the value column to a string
    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # Define a schema for the orders data
    # order_id,order_product_name,order_card_type,order_amount,order_datetime,order_country_name,order_city_name,order_ecommerce_website_name
    orders_schema = StructType() \
        .add("order_id", StringType()) \
        .add("order_product_name", StringType()) \
        .add("order_card_type", StringType()) \
        .add("order_amount", StringType()) \
        .add("order_datetime", StringType()) \
        .add("order_country_name", StringType()) \
        .add("order_city_name", StringType()) \
        .add("order_ecommerce_website_name", StringType())

    # Apply the schema to the orders data and select the orders data and the timestamp column
    orders_df2 = orders_df1\
        .select(from_json(col("value"), schema = orders_schema)\
        .alias("orders"), "timestamp")
    
     # Select the columns from the orders data and the timestamp column
    orders_df3 = orders_df2.select("orders.*", "timestamp")

     # Group the data by time window and count the number of records in each window
     # Here the time window is 10 seconds and sliding interval is taken as 5 seconds
     # agg function computes the count of all rows in each time window
     # We will display only the timestamp and number of records streamed in every window for easier interpretation
    count_df = orders_df3 \
    .groupBy(window(col("timestamp"), "10 seconds", "5 seconds").alias("Time Window")) \
    .agg(count("*").alias("Record Count"))


    # Write final result into console for debugging purpose
    orders_agg_write_stream = count_df \
        .writeStream \
        .trigger(processingTime='0 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()
    
    

    orders_agg_write_stream.awaitTermination()

    print("Stream Data Processing Application Completed.")
