import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min
from matplotlib import pyplot as plt


# Usage: spark-submit Top10cara.py <dataset dir>

spark = SparkSession.builder.appName("Top10barata").getOrCreate()

result = spark.createDataFrame([("",0.0)], schema=["Company", "Min Price"])

for file in os.listdir(sys.argv[1]):
    if file.endswith(".csv"):
        df = spark.read.option("header", "true").csv(os.path.join(sys.argv[1], file))

        minprice = df.agg(min(col("Low"))).collect()[0][0]
        minprice = round(float(minprice), 3)

        result = result.union(spark.createDataFrame([(file.split('.csv')[0], minprice)], schema=["Company", "Min Price"]))

result = result.orderBy(col("Min Price").asc()).limit(10)

result.show()

pand = result.toPandas()
plt.bar(pand["Company"], pand["Min Price"])
plt.xlabel('Company')
plt.ylabel('Min Price')
plt.title('Top 10 acciones más baratas')
plt.xticks(rotation=45, ha="right")
plt.savefig('Top 10 acciones más baratas.png')

spark.stop()