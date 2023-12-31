#!/usr/bin/python3
import os
import sys
from pyspark.sql import SparkSession, functions as f, Window
from matplotlib import pyplot as plt


def top5HighestGrowthChinaYear(dir,year):
    spark = SparkSession.builder.appName("top5HighestGrowthChinaYear").getOrCreate()
    
    result = []
    for file in os.listdir(dir):
        if file.endswith(".csv"):
            df = spark.read.csv("{}/{}".format(dir,file),header=True,inferSchema=True)
    
            df = df.select(df[0],df.close)
            
            df = df.withColumn("date", f.to_date(df[0]))
            
            df = df.groupBy(f.year("date").alias("Year"),f.month("date").alias("Month")).agg(f.avg("close").alias("close")).orderBy(f.col("Year"),f.col("Month"))#agrupamos por año y mes y hacemos el valor medio
            df= df.withColumn("n",f.lit(1))# esto es para usarlo en window
            df = df.where(df.Year==year)
            window = Window.partitionBy("n").orderBy(df.Year, df.Month)
            df = df.withColumn("prev_value", f.lag(f.col("close")).over(window)) # creamos una columna con el valor anterior de close

            df = df.where(df.prev_value.isNull() |(df.close>df.prev_value))#quitamos las filas que este vacio el valor inicial 
            df = df.drop("n").drop("prev_value")
            max_grow = 0.0 
            datas = df.collect()
            ini_pos = 0
            if(len(datas) != 0):
                ini_value=datas[0].close

            for i in range(1,len(datas)):
                try:
                    if abs(datas[i-1].Month-datas[i].Month)%10!=1:
                        ini_pos=i-1
                        ini_value=datas[i].prev_value
                    grow=((datas[i].close-ini_value)/ini_value*100)/(i-ini_pos)
                            
                    max_grow = max(grow,max_grow)
                except:
                    continue

            result.append((file.split(".")[0], round(max_grow, 2)))
        
                
    result = sorted(result, key=lambda x : x[1], reverse=True)
    
    spark.stop()

    return result[:5]    


def generateImg(result, path = "./"):
    Company, Price = zip(*result)
    plt.bar(Company, Price)
    plt.xlabel('Company')
    plt.ylabel('Max Growth')
    plt.title('top5HighestGrowthChinaYear')
    plt.xticks(rotation=45, ha="right")
    plt.savefig(os.path.join(path, 'top5HighestGrowthChinaYear.png'))


if __name__ == "__main__":
    if(len(sys.argv)<3):
        print("Usage: spark-submit top5HighestGrowthChinaYear.py <dataset dir> <year>")
        exit(0)
    dir = sys.argv[1]
    
    result = top5HighestGrowthChinaYear(dir,sys.argv[2])
    print(result)

    generateImg(result)

