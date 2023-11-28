import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min
from pyspark.sql.types import DateType

def predictionUSYear(dir, year):
    spark = SparkSession.builder.appName("predictionUSYear").getOrCreate()

    result = 0
    count = 0

    for file in os.listdir(dir):
        if file.endswith(".csv"):
            count += 1

            df = spark.read.option("header", "true").csv(os.path.join(dir, file))

            df = df.withColumn("Date", col("Date").cast(DateType()))
            df = df.withColumn("Close", col("Close").cast("float"))

            # Filtra por el año especificado
            # df_filtrado = df.filter(col("Date").between(f"{year}-01-01", f"{year}-12-31"))

            # if df_filtrado.isEmpty() == False:
            precio_fecha_mas_pequeña = df.collect()[0]["Close"]
            precio_fecha_mas_grande = df.orderBy(col("Date").desc()).collect()[0]["Close"]

            if (precio_fecha_mas_grande - precio_fecha_mas_pequeña > 0):
                result += 1

    probabilidad = (result / count) * 100

    spark.stop()

    return probabilidad