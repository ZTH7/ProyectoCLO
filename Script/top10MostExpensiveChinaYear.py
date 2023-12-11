import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import pyspark.sql.functions as f
from matplotlib import pyplot as plt


# Usage: spark-submit top10MostExpensiveChinaYear.py <dataset dir> <year>

def top10MostExpensiveChinaYear(dir, year):
    spark = SparkSession.builder.appName("top10MostExpensiveChinaYear").getOrCreate()

    result = []

    for file in os.listdir(dir):
        if file.endswith(".csv"):
            try:
                df = spark.read.option("header", "true").csv(os.path.join(dir, file))

                df = df.withColumn("date", to_date(df[0]))
                df = df.withColumn("close", col("close").cast("float"))
                df = df.groupBy(f.year("date").alias("Year")).max("close")
                df = df.where(df.Year==year)

                maxprice = df.select("max(close)").collect()[0][0]
                maxprice = round(float(maxprice), 3)

                result.append((file.split(".")[0], maxprice))
            except:
                continue

    result = sorted(result, key=lambda x : x[1], reverse=True)

    spark.stop()

    return result[:10]

def generateImg(result, year, path = "./"):
    Company, Price = zip(*result)
    plt.bar(Company, Price)
    plt.xlabel('Company')
    plt.ylabel('Max Price')
    plt.title('top 10 Most Expensive China Year '+year)
    plt.xticks(rotation=45, ha="right")
    plt.savefig(os.path.join(path, 'top10MostExpensiveChinaYear'+year+'.png'))

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: spark-submit top10MostExpensiveChinaYear.py <dataset dir> <year>")
        exit(0)

    result = top10MostExpensiveChinaYear(sys.argv[1], sys.argv[2])
    print(result)

    if len(sys.argv) > 3:
        generateImg(result, sys.argv[2], sys.argv[3])
    else:
        generateImg(result, sys.argv[2])
    