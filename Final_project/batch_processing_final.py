#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
import pyspark.sql.types as T

from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import DecisionTreeClassifier,NaiveBayes,LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.ml import PipelineModel
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import NGram,OneHotEncoder, StringIndexer, VectorAssembler


# In[ ]:


from pyspark.ml.stat import Correlation
from pyspark.ml import PipelineModel
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import NGram,OneHotEncoder, StringIndexer, VectorAssembler
from itertools import combinations
import sparknlp


# In[ ]:


spark = sparknlp.start()


# In[ ]:


df = spark.read.csv("gs://bdl2023-final-proj/YELP_train.csv")


# In[ ]:


df = df.withColumn("_c1", col("_c1").cast("int"))
df = df.withColumn("_c3", col("_c3").cast("int"))
df = df.withColumn("_c5", col("_c5").cast("int"))
df = df.withColumn("_c7", col("_c7").cast("int"))


# In[ ]:


df.show(5)


# In[ ]:


df = df.dropna(subset = ['_c5'])
df = df.dropna(subset = ['_c6'])
df = df.drop('_c0','_c4','_c8')


# In[ ]:


# !pip install tabulate


# In[ ]:


df = df.withColumn("_c2", to_timestamp("_c2", "yyyy-MM-dd HH:mm:ss"))


# In[ ]:


df = df.withColumn("hour", hour("_c2"))
df = df.withColumn("category", when((df.hour >= 6) & (df.hour < 12), "morning")
                           .when((df.hour >= 12) & (df.hour < 18), "afternoon")
                           .when((df.hour >= 18) & (df.hour < 22), "evening")
                           .otherwise("night"))

# drop "hour" column
df = df.drop("hour")


# In[ ]:


df = df.withColumn("year", year("_c2"))
df = df.withColumn("day_of_week", dayofweek(df._c2))


# In[ ]:


# df.dtypes


# In[ ]:


df = df.drop('weekday')
df = df.withColumn("is_weekend", when((df.day_of_week == 1) | (df.day_of_week == 7), 1)
                           .otherwise(0))


# In[ ]:


indexer_day = StringIndexer(inputCol="category", outputCol="part_of_day")
df = indexer_day.fit(df).transform(df)


# In[ ]:


df = df.withColumn("month", month("_c2"))

df = df.drop('_c2')
df = df.drop('category')


# In[ ]:


df = df.withColumnRenamed('_c5','rating')
df = df.withColumnRenamed('_c6','review')


# In[ ]:


for col_name in df.columns:
    try:
        median_value = df.approxQuantile(col_name, [0.5], 0.01)[0]
        df = df.fillna(median_value, subset=[col_name])
    except:
        pass


# In[ ]:


df = df.filter(col('rating')>=0)
df = df.filter(col('rating')<=5)

df = df.filter(col('_c1')>=0)
df = df.filter(col('_c3')>=0)
df = df.filter(col('_c7')>=0)


# In[ ]:


df.show()


# In[ ]:


df.write.format('csv').mode('overwrite').option("header", "true").save('gs://final_2023_project/data_final.csv')


# In[ ]:


model = PipelineModel.load('gs://project_2023/tfidf_model')


# In[ ]:


df = df.withColumn("_c1", col("_c1").cast("int"))
df = df.withColumn("_c3", col("_c3").cast("int"))
df = df.withColumn("rating", col("rating").cast("double"))
df = df.withColumn("_c7", col("_c7").cast("int"))
df = df.withColumn("year", col("year").cast("int"))
df = df.withColumn("day_of_week", col("day_of_week").cast("int"))
df = df.withColumn("is_weekend", col("is_weekend").cast("int"))
df = df.withColumn("part_of_day", col("part_of_day").cast("int"))
df = df.withColumn("month", col("month").cast("int"))


# In[ ]:


processed_df = model.transform(df)


# In[ ]:


processed_df.show(5)


# In[ ]:


feature_cols = processed_df.drop('review','to_spark','filtered','raw_features','rating').columns


# In[ ]:


pipeline_nb_fit = PipelineModel.load('gs://project_2023/nb_model')


# In[ ]:


evaluator = MulticlassClassificationEvaluator(predictionCol = "prediction",labelCol="rating")
prediction = pipeline_nb_fit.transform(processed_df)
prediction.select("prediction", "rating").show(5)
print("Evaluating on training data using NB classifier (f1 score):", evaluator.evaluate(prediction, {evaluator.metricName: "f1"}))
print("Evaluating on training data using NB classifier (Accuracy):", evaluator.evaluate(prediction, {evaluator.metricName: "accuracy"}))


# In[ ]:




