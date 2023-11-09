import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
from matplotlib import pyplot as plt


# Usage: spark-submit Top10cara.py <dataset dir>

spark = SparkSession.builder.appName("Top10cara").getOrCreate()

result = spark.createDataFrame([("",0.0)], schema=["Company", "Max Price"])

for file in os.listdir(sys.argv[1]):
    if file.endswith(".csv"):
        df = spark.read.option("header", "true").csv(os.path.join(sys.argv[1], file))

        maxprice = df.agg(max(col("High"))).collect()[0][0]
        maxprice = round(float(maxprice), 3)

        result = result.union(spark.createDataFrame([(file.split('.csv')[0], maxprice)], schema=["Company", "Max Price"]))

result = result.orderBy(col("Max Price").desc()).limit(10)

result.show()

pand = result.toPandas()
plt.bar(pand["Company"], pand["Max Price"])
plt.xlabel('Company')
plt.ylabel('Max Price')
plt.title('Top 10 acciones más caras')
plt.xticks(rotation=45, ha="right")
plt.savefig('Top 10 acciones más caras.png')

spark.stop()