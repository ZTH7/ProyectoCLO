import os
import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, udf
from matplotlib import pyplot as plt


# Usage: spark-submit top10MostExpensiveIndiaYear.py <dataset dir> <year>

def top10MostExpensiveIndiaYear(dir, year):
    spark = SparkSession.builder.appName("top10MostExpensiveIndiaYear").getOrCreate()

    result = []

    for file in os.listdir(dir):
        if file.endswith(".csv"):
            df = spark.read.option("header", "true").csv(os.path.join(dir, file))

            df = df.withColumn("date", udf((lambda line : re.sub(r'-(.*)', '', line)))("date"))
            df = df.withColumn("close", col("close").cast("float"))
            df = df.groupBy("date").max("close")

            maxprice = df.filter(col("date") == year).select("max(close)").collect()[0][0]
            maxprice = round(float(maxprice), 3)

            result.append((file.split(".")[0], maxprice))

    result = sorted(result, key=lambda x : x[1], reverse=True)

    #print(result[:10])
    spark.stop()

    return result[:10]

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: spark-submit top10MostExpensiveIndiaYear.py <dataset dir> <year>")

    result = top10MostExpensiveIndiaYear(sys.argv[1], sys.argv[2])
    Company, Price = zip(*result)

    plt.bar(Company, Price)
    plt.xlabel('Company')
    plt.ylabel('Max Price')
    plt.title('top10MostExpensiveIndiaYear.png')
    plt.xticks(rotation=45, ha="right")
    plt.savefig('top10MostExpensiveIndiaYear.png')