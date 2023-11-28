import os
import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, udf
from matplotlib import pyplot as plt


# Usage: spark-submit top10MostExpensiveUSYear.py <dataset dir> <year>

def top10MostExpensiveUSYear(dir, year):
    spark = SparkSession.builder.appName("top10MostExpensiveUSYear").getOrCreate()

    result = []

    for file in os.listdir(dir):
        if file.endswith(".csv"):
            df = spark.read.option("header", "true").csv(os.path.join(dir, file))

            df = df.withColumn("Date", udf((lambda line : re.sub(r'(.*)-', '', line)))("Date"))
            df = df.withColumn("Close", col("Close").cast("float"))
            df = df.groupBy("Date").max("Close")

            maxprice = df.filter(col("Date") == year).select("max(Close)").collect()[0][0]
            maxprice = round(float(maxprice), 3)

            result.append((file.split(".")[0], maxprice))

    result = sorted(result, key=lambda x : x[1], reverse=True)

    #print(result[:10])
    spark.stop()

    return result[:10]

""" if len(sys.argv) < 3:
    print("Usage: spark-submit top10MostExpensiveUSYear.py <dataset dir> <year>")

result = top10MostExpensiveUSYear(sys.argv[1], sys.argv[2])

Company, Price = zip(*result)
plt.bar(Company, Price)
plt.xlabel('Company')
plt.ylabel('Max Price')
plt.title('top10MostExpensiveUSYear.png')
plt.xticks(rotation=45, ha="right")
plt.savefig('top10MostExpensiveUSYear.png') """