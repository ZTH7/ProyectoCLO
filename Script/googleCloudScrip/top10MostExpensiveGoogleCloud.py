from google.cloud import storage
import sys
from pyspark.sql.functions import col, max
from pyspark.sql import SparkSession


def top10MostExpensiveUS(list_dir):

    spark = SparkSession.builder.appName("top10MostExpensiveUS").getOrCreate()

    result = []

    for file in list_dir:
        if file.endswith(".csv"):
            df = spark.read.format("csv").option("header", "true").load(file)
            maxprice = df.agg(max(col("Close"))).collect()[0][0]
            maxprice = round(float(maxprice), 3)

            result.append((file.split(".")[0], maxprice))

    result = sorted(result, key=lambda x : x[1], reverse=True)

    spark.stop()

    return result[:10]


def top10MostExpensiveChina(list_dir):

    spark = SparkSession.builder.appName("top10MostExpensiveChina").getOrCreate()

    result = []

    for file in list_dir:
        if file.endswith(".csv"):
            try:
                df = spark.read.format("csv").option("header", "true").load(file)

                maxprice = df.agg(max(col("close"))).collect()[0][0]
                maxprice = round(float(maxprice), 3)

                result.append((file.split(".")[0], maxprice))
            except:
                continue

    result = sorted(result, key=lambda x : x[1], reverse=True)
    
    spark.stop()

    return result[:10]

def top10MostExpensiveIndia(list_dir):

    spark = SparkSession.builder.appName("top10MostExpensiveIndia").getOrCreate()

    result = []

    for file in list_dir:
        if file.endswith(".csv"):
            df = spark.read.format("csv").option("header", "true").load(file)

            maxprice = df.agg(max(col("close"))).collect()[0][0]
            maxprice = round(float(maxprice), 3)

            result.append((file.split("_")[0], maxprice))

    result = sorted(result, key=lambda x : x[1], reverse=True)

    spark.stop()

    return result[:10]

# Usage: spark-submit top10MostExpensive.py <dataset dir>

def top10MostExpensive(bucket_name):
    
    list_dirUS= []
    list_dirIndia= []
    list_dirChina= []
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs_all = list(bucket.list_blobs())
    for blob in blobs_all:  
        file = f"gs://{bucket_name}/{blob.name}"
        if blob.name.endswith('.csv'):
            if blob.name.startswith('Samples/US_data') :
                list_dirUS.append(file)
            elif blob.name.startswith('Samples/China_data'):
                list_dirChina.append(file)
            elif blob.name.startswith('Samples/India_data'):
                list_dirIndia.append(file)        
    result=[]
    result = top10MostExpensiveUS(list_dirUS)
    result += top10MostExpensiveChina(list_dirChina)
    result += top10MostExpensiveIndia(list_dirIndia)

    result = sorted(result, key=lambda x : x[1], reverse=True)

    return result[:10]


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit top10MostExpensive.py <bucket_name>")

    result = top10MostExpensive(sys.argv[1])
    print(result)

    
