import os
import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, udf
from matplotlib import pyplot as plt


# Usage: spark-submit top10MostExpensiveChinaYear.py <dataset dir> <year>

def top10MostExpensiveChinaYear(dir, year):
    spark = SparkSession.builder.appName("top10MostExpensiveChinaYear").getOrCreate()

    result = []

    for file in os.listdir(dir):
        if file.endswith(".csv"):
            try:
                df = spark.read.option("header", "true").csv(os.path.join(dir, file))

                df = df.filter(col("_c0") == year)
                df = df.withColumn("_c0", udf((lambda line : re.sub(r'-(.*)', '', line)))("_c0"))
                df = df.withColumn("close", col("close").cast("float"))
                df = df.groupBy("_c0").max("close")

                maxprice = df.select("max(close)").collect()[0][0]
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
    plt.title('top10MostExpensiveChinaYear.png')
    plt.xticks(rotation=45, ha="right")
    plt.savefig(os.path.join(path, 'top10MostExpensiveChinaYear.png'))

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: spark-submit top10MostExpensiveChinaYear.py <dataset dir> <year>")
        exit(0)

    result = top10MostExpensiveChinaYear(sys.argv[1], sys.argv[2])
    print(result)

    if len(sys.argv) > 3:
        generateImg(result,sys.argv[3])
    else:
        generateImg(result)
    