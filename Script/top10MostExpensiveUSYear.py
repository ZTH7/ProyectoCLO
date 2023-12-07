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
            try:
                df = spark.read.option("header", "true").csv(os.path.join(dir, file))

                df = df.filter(col("Date") == year)
                df = df.withColumn("Date", udf((lambda line : re.sub(r'(.*)-', '', line)))("Date"))
                df = df.withColumn("Close", col("Close").cast("float"))
                df = df.groupBy("Date").max("Close")

                maxprice = df.select("max(Close)").collect()[0][0]
                maxprice = round(float(maxprice), 3)

                result.append((file.split(".")[0], maxprice))
            except:
                continue

    result = sorted(result, key=lambda x : x[1], reverse=True)

    spark.stop()

    return result[:10]

def generateImg(result, path = "./"):
    Company, Price = zip(*result)
    plt.bar(Company, Price)
    plt.xlabel('Company')
    plt.ylabel('Max Price')
    plt.title('top10MostExpensiveUSYear.png')
    plt.xticks(rotation=45, ha="right")
    plt.savefig(os.path.join(path, 'top10MostExpensiveUSYear.png'))

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: spark-submit top10MostExpensiveUSYear.py <dataset dir> <year>")

    result = top10MostExpensiveUSYear(sys.argv[1], sys.argv[2])
    print(result)

    if len(sys.argv) > 3:
        generateImg(result,sys.argv[3])
    else:
        generateImg(result)