import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
from matplotlib import pyplot as plt


# Usage: spark-submit top10MostExpensiveUS.py <dataset dir>

def top10MostExpensiveUS(dir):

    spark = SparkSession.builder.appName("top10MostExpensiveUS").getOrCreate()

    result = []

    for file in os.listdir(dir):
        if file.endswith(".csv"):
            df = spark.read.option("header", "true").csv(os.path.join(dir, file))

            maxprice = df.agg(max(col("Close"))).collect()[0][0]
            maxprice = round(float(maxprice), 3)

            result.append((file.split(".")[0], maxprice))

    result = sorted(result, key=lambda x : x[1], reverse=True)

    #print(result[:10])

    spark.stop()

    return result[:10]

""" if len(sys.argv) < 2:
    print("Usage: spark-submit top10MostExpensiveUS.py <dataset dir>")


result = top10MostExpensiveUS(sys.argv[1])
Company, Price = zip(*result)
plt.bar(Company, Price)
plt.xlabel('Company')
plt.ylabel('Max Price')
plt.title('top10MostExpensiveUS')
plt.xticks(rotation=45, ha="right")
plt.savefig('top10MostExpensiveUS.png') """