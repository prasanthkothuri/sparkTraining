# required spark imports

from pyspark import SparkConf, SparkContext

from pyspark.sql import SparkSession

from pyspark.sql.functions import explode

from pyspark.sql.functions import split
conf = SparkConf().setMaster("local[*]")

sc = SparkContext(conf = conf)

spark = SparkSession(sc)
lines = spark.readStream.format("socket").option("host","localhost").option("port",7878).load()
words = lines.select(

   explode(

       split(lines.value, " ")

   ).alias("word")

)
wordCounts = words.groupBy("word").count()
query = wordCounts.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()
