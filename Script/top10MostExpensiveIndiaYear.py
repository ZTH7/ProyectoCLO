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
            try:
                df = spark.read.option("header", "true").csv(os.path.join(dir, file))

                df = df.filter(col("date") == year)
                df = df.withColumn("date", udf((lambda line : re.sub(r'-(.*)', '', line)))("date"))
                df = df.withColumn("close", col("close").cast("float"))
                df = df.groupBy("date").max("close")

                maxprice = df.select("max(close)").collect()[0][0]
                maxprice = round(float(maxprice), 3)

                result.append((file.split("_")[0], maxprice))
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
    plt.title('top10MostExpensiveIndiaYear.png')
    plt.xticks(rotation=45, ha="right")
    plt.savefig(os.path.join(path, 'top10MostExpensiveIndiaYear.png'))

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: spark-submit top10MostExpensiveIndiaYear.py <dataset dir> <year>")
        exit(0)

    result = top10MostExpensiveIndiaYear(sys.argv[1], sys.argv[2])
    print(result)

    if len(sys.argv) > 3:
        generateImg(result,sys.argv[3])
    else:
        generateImg(result)
    