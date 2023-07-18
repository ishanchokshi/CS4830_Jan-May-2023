from pyspark.sql.functions import lit, col, when
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# I have kept the original comments and slightly modified them

spark = SparkSession.builder.appName("SCDTypeII").getOrCreate()
# Given a DataFrame of customer records, where validity_start is set to a date from a long time ago - e.g. 01-01-1970
# while validity_end will be set to a long time in the future such that there is only 1 record per customer that is valid as on date

## Creating the master dataframe:
customer_master = [[1,'Harsha','20-08-1990','01-01-1970','12-12-9999'],[2,'Goldie','11-02-1990','01-01-1970','12-12-9999'],[3,'Divya','25-12-1990','01-01-1970','12-12-9999']]
cm_df = spark.createDataFrame(customer_master, ['id', 'name', 'dob', 'validity_start', 'validity_end'])

# ... and given some updates to some of the records as of now

# Creating the updating dataframe
updates = [['Harsha','05-09-1990']]
updates_df = spark.createDataFrame(updates, ['name', 'updated_dob'])
now = '12-03-2023'

# ... create the updated customer records DataFrame such that
# both the previous version and new version of a record are tracked using timestamp of update

#  Function to create the current version of the full customer record
def fullrowupdates(updates_df,cm_df):
    fullrowupdates_df = updates_df.join(cm_df, 'name', 'inner')
    fullrowupdates_df = fullrowupdates_df.drop('dob').withColumnRenamed('updated_dob', 'dob')
    fullrowupdates_df = fullrowupdates_df.withColumn('validity_start', lit(now).cast(StringType()))
    return fullrowupdates_df

# Function to update the previous record to close the validity
def closeprev(updates_df,cm_df):
    closeprev_df = cm_df.join(updates_df, 'name', 'left_outer')
    closeprev_df = closeprev_df.withColumn('validity_end', when(col('updated_dob').isNull(), col('validity_end')).otherwise(lit(now).cast(StringType())))
    closeprev_df = closeprev_df.drop('updated_dob')
    return closeprev_df

# Function to set the final customer master as a combination of old and new records
def final_join(closeprev_df,fullrowupdates_df):
    cm_df = closeprev_df.unionByName(fullrowupdates_df)
    cm_df_1 = cm_df.select('id', 'name', 'dob', 'validity_start', 'validity_end')
    return cm_df_1

fullrowupdates_df = fullrowupdates(updates_df,cm_df)
closeprev_df = closeprev(updates_df,cm_df)
updated_df = final_join(closeprev_df,fullrowupdates_df)

updated_df.show()