import os
import sys
from pyspark.sql.functions import col, max, min
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession, functions as f, types ,Window

def predictionUSYear(dir, year):
    spark = SparkSession.builder.appName("predictionUSYear").getOrCreate()

    result = 0
    count = 0

    for file in os.listdir(dir):
        if file.endswith(".csv"):
            count += 1

            df = spark.read.option("header", "true").csv(os.path.join(dir, file))

            df = df.withColumn("Date",f.to_date(f.col("Date"),"dd-MM-yyyy"))# convertimos la columna date en el tipo date
            df = df.orderBy(f.year("Date").alias("Year"),f.month("Date").alias("Month")) #agrupamos por año y mes

            df = df.withColumn("Close", col("Close").cast("float"))

            # Filtra por el año especificado
            df_filtrado = df.filter(f.year("Date").alias("Year") == year)

            if df_filtrado.isEmpty() == False:
                precio_fecha_mas_pequeña = df_filtrado.collect()[0]["Close"]
                precio_fecha_mas_grande = df_filtrado.orderBy(col("Date").desc()).collect()[0]["Close"]
                if (precio_fecha_mas_grande - precio_fecha_mas_pequeña > 0):
                    result += 1

    probabilidad = (result / count) * 100

    spark.stop()

    return probabilidad


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: spark-submit predictionUSYear.py <dataset dir> <year>")

    result = predictionUSYear(sys.argv[1], sys.argv[2])
    print(result)

