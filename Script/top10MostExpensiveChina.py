import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
from matplotlib import pyplot as plt


# Usage: spark-submit top10MostExpensiveChina.py <dataset dir>

def top10MostExpensiveChina(dir):

    spark = SparkSession.builder.appName("top10MostExpensiveChina").getOrCreate()

    result = []

    for file in os.listdir(dir):
        if file.endswith(".csv"):
            df = spark.read.option("header", "true").csv(os.path.join(dir, file))

            maxprice = df.agg(max(col("close"))).collect()[0][0]
            maxprice = round(float(maxprice), 3)

            result.append((file.split(".")[0], maxprice))

    result = sorted(result, key=lambda x : x[1], reverse=True)
    
    spark.stop()

    return result[:10]

def generateImg(result, path = "./"):
    Company, Price = zip(*result)
    plt.bar(Company, Price)
    plt.xlabel('Company')
    plt.ylabel('Max Price')
    plt.title('top10MostExpensiveChina')
    plt.xticks(rotation=45, ha="right")
    plt.savefig(os.path.join(path, 'top10MostExpensiveChina.png'))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit top10MostExpensiveChina.py <dataset dir>")
        exit(0)

    result = top10MostExpensiveChina(sys.argv[1])
    print(result)

    if len(sys.argv) > 2:
        generateImg(result,sys.argv[2])
    else:
        generateImg(result)
    