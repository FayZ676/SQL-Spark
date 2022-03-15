from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import pyspark.sql.types as T

spark = SparkSession.builder.master("local[*]") \
        .config('spark.jar.packages', 'gcs-connector-hadoop2-latest.jar') \
        .getOrCreate()

# Setup hadoop fs configuration for schema gs://
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

start_time = datetime.now()

path = "gs://projectse413/wikiviewpages/wikifile_*.json"

jsonSchema = T.StructType([
        T.StructField('datehour' ,T.DateType(),True),
        T.StructField('wiki' ,T.StringType(),True),
        T.StructField('title' ,T.StringType(),True),
        T.StructField('views' ,T.IntegerType(),True)
])

df = spark.read.json(path, jsonSchema)

FacebookDF = df.filter(col("title").contains("Facebook"))
GoogleDF = df.filter(col("title").contains("Google"))
WikipediaDF = df.filter(col("title").contains("Wikipedia"))

print("Total number of Facebook keyword: ", FacebookDF.count())
print("Total number of Google keyword: ", GoogleDF.count())
print("Total number of Wikipedia keyword: ", WikipediaDF.count())


end_time = datetime.now()

print('Duration: {}'.format(end_time - start_time))
