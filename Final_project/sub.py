from pyspark import SparkContext, SQLContext
from itertools import chain
from pyspark.ml import PipelineModel, Pipeline
import pyspark
# import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.tuning import CrossValidatorModel
from pyspark.sql import SparkSession
# from pyspark.sql.functions import split, decode, substring
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# from pyspark.sql.functions import from_json, udf, split
from kafka import KafkaConsumer
import json
import pandas as pd
from sparknlp import *
import sparknlp


def start_consuming():
    spark = sparknlp.start()
    print('Importing NLP Model')
    # model = PipelineModel.load('LogReg_TFIDF_model_final')
    model = PipelineModel.load('gs://project_2023/tfidf_model')
    print('Done loading the NLP model.')
    
    print('Importing the ML model')
    ml_model = PipelineModel.load('gs://project_2023/nb_model')
    print('Done loading the ML model')
    

    df_schema = StructType() \
        .add("_c1", StringType()) \
        .add("_c3", StringType()) \
        .add("rating", StringType()) \
        .add("review", StringType()) \
        .add("_c7", StringType()) \
        .add("year", StringType()) \
        .add("day_of_week", StringType()) \
        .add("order_ecommerce_website_name", StringType()) \
        .add("is_weekend", StringType()) \
        .add("part_of_day", StringType()) \
        .add("month", StringType())


    ctr = 0
    print('Spark session active.')
    consumer = KafkaConsumer('yelp-nlp', bootstrap_servers = ['localhost:9092'])
    for msg in consumer:
        message = json.loads(msg.value)
        df_line = pd.json_normalize(message)
        if ctr == 0:
            df = df_line.copy()
            df_line = spark.createDataFrame(df_line, schema=df_schema,header=True)
            df = spark.createDataFrame(df, schema = df_schema,header=True)
        else:
            df = df.toPandas()
            df = df.append(df_line, ignore_index= True)
            df_line = spark.createDataFrame(df_line, schema = df_schema)
            df = spark.createDataFrame(df, schema = df_schema,header=True)
        ctr += 1
        
        


        df = df.withColumn("_c1", col("_c1").cast("int"))
        df = df.withColumn("_c3", col("_c3").cast("int"))
        df = df.withColumn("rating", col("rating").cast("double"))
        df = df.withColumn("_c7", col("_c7").cast("int"))
        df = df.withColumn("year", col("year").cast("int"))
        df = df.withColumn("day_of_week", col("day_of_week").cast("int"))
        df = df.withColumn("is_weekend", col("is_weekend").cast("int"))
        df = df.withColumn("part_of_day", col("part_of_day").cast("int"))
        df = df.withColumn("month", col("month").cast("int"))
        nlp_df = model.transform(df)
        prediction = ml_model.transform(nlp_df)
        prediction = prediction.select(['rating', 'prediction'])
        prediction.write.format("console").save()
        # output_df = prediction.withColumn("correct", f.when((f.col('prediction')==1.0) & (f.col('rating')==1.0),1).when((f.col('prediction')==2.0) & (f.col('rating')==2.0),1).when((f.col('rating')==3.0) & (f.col('rating')==3.0),1).when((f.col('prediction')==4.0) & (f.col('rating')==4.0),1).when((f.col('prediction')==5.0) & (f.col('rating')==5.0),1).otherwise(0))

        f1_eval = MulticlassClassificationEvaluator(labelCol="rating", predictionCol="prediction", metricName='f1')
        evaluator = MulticlassClassificationEvaluator(labelCol="rating", predictionCol="prediction", metricName="accuracy")
        acc = evaluator.evaluate(prediction) * 100
        f1 = f1_eval.evaluate(prediction)
        batch = ctr + 1
        print("Accuracy and F1-score for batch {}: ".format(batch), acc, f1)
        print("------------------------")

start_consuming()