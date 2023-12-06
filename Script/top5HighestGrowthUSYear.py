#!/usr/bin/python3

import os
import sys
from tarfile import NUL
from pyspark.sql import SparkSession, functions as f, types ,Window


def top5HighestGrowthYear(dir,year):
    spark = SparkSession.builder.appName("top5HighestGrowthYear").getOrCreate()
    
    result = []
    for file in os.listdir(dir):
        if file.endswith(".csv"):
            df = spark.read.csv("{}/{}".format(dir,file),header=True,inferSchema=True)
            df = df.select(f.col("Date"),f.col("close"))#selecionamos las columnas
            df = df.withColumn("Date",f.to_date(f.col("Date"),"dd-MM-yyyy"))# convertimos la columna date en el tipo date
            df = df.groupBy(f.year("Date").alias("Year"),f.month("Date").alias("Month")).agg(f.avg("Close").alias("Close")).orderBy(f.col("Year"),f.col("Month"))#agrupamos por año y mes y hacemos el valor medio
            df = df.where(df.Year== year)
       
            df= df.withColumn("n",f.lit(1))# esto es para usarlo en window

            window = Window.partitionBy("n").orderBy(df.Year, df.Month)
            df = df.withColumn("prev_value", f.lag(f.col("Close")).over(window)) # creamos una columna con el valor anterior de Close
            df = df.where(df.prev_value.isNull() |(df.Close>df.prev_value))#quitamos las filas que este vacio el valor inicial 
            df = df.drop("n")
            max_grow = 0 
            datas = df.collect()
            ini_pos = 0
            if(len(datas) != 0):
                ini_value=datas[0].Close

            for i in range(1,len(datas)):
                if abs(datas[i-1].Month-datas[i].Month)%10!=1:
                    ini_pos=i-1
                    ini_value=datas[i].prev_value
                grow=((datas[i].Close-ini_value)/ini_value*100)/(i-ini_pos)
                         
                max_grow = max(grow,max_grow)    

            result.append((os.path.splitext(file)[0], round(max_grow, 2)))
        
    result = sorted(result, key=lambda x : x[1], reverse=True)
    
    spark.stop()

    return result[:5]    


if __name__ == "__main__":
    if(len(sys.argv)<3):
        print("Usage: spark-submit top5HighestGrowthUSYear <dataset dir> <year>")
        exit(0)
    dir = sys.argv[1]
    print(top5HighestGrowthYear(dir,sys.argv[2]))





