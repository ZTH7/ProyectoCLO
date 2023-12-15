
import sys
from pyspark.sql import SparkSession, functions as f, Window
from google.cloud import storage



def top5HighestGrowthIndia(list_dir):
    spark = SparkSession.builder.appName("top5HighestGrowthIndia").getOrCreate()
    
    result = []
    
    for file in list_dir:
        if file.endswith(".csv"):
            df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file)
            df = df.select(df.date,df.close)
            df = df.withColumn("date", f.to_date(df.date))
            
            df = df.groupBy(f.year("date").alias("Year"),f.month("date").alias("Month")).agg(f.avg("close").alias("close")).orderBy(f.col("Year"),f.col("Month"))#agrupamos por año y mes y hacemos el valor medio
            df= df.withColumn("n",f.lit(1))# esto es para usarlo en window

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

            result.append((file.split("_")[0], round(max_grow, 2)))
        
    result = sorted(result, key=lambda x : x[1], reverse=True)
    
    spark.stop()

    return result[:5]    

def top5HighestGrowthUS(list_dir):
    spark = SparkSession.builder.appName("top5HighestGrowthUS").getOrCreate()
    
    result = []
    for file in list_dir:
        if file.endswith(".csv"):
            df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file)
            df = df.select(f.col("Date"),f.col("Close"))#selecionamos las columnas
            df = df.withColumn("Date",f.to_date(f.col("Date"),"dd-MM-yyyy"))# convertimos la columna date en el tipo date
            df = df.groupBy(f.year("Date").alias("Year"),f.month("Date").alias("Month")).agg(f.avg("Close").alias("Close")).orderBy(f.col("Year"),f.col("Month"))#agrupamos por año y mes y hacemos el valor medio
            df= df.withColumn("n",f.lit(1))# esto es para usarlo en window

            window = Window.partitionBy("n").orderBy(df.Year, df.Month)
            df = df.withColumn("prev_value", f.lag(f.col("Close")).over(window)) # creamos una columna con el valor anterior de Close
            df = df.where(df.prev_value.isNull() |(df.Close>df.prev_value))#quitamos las filas que este vacio el valor inicial 
            df = df.drop("n")
            max_grow = 0 
            datas = df.collect()     
            ini_pos = 0 
            if(len(datas)!= 0):
                ini_value=datas[0].Close
            for i in range(1,len(datas)):
                try:
                    if abs(datas[i-1].Month-datas[i].Month)%10!=1:
                        ini_pos=i-1
                        ini_value=datas[i].prev_value
                    grow=((datas[i].Close-ini_value)/ini_value*100)/(i-ini_pos)
                    max_grow = max(grow,max_grow)    
                except:
                    continue

            result.append((file.split(".")[0], round(max_grow, 2)))
    result = sorted(result, key=lambda x : x[1], reverse=True)
    
    spark.stop()

    return result[:5] 

def top5HighestGrowthChina(list_dir):
    spark = SparkSession.builder.appName("top5HighestGrowthChina").getOrCreate()
    
    result = []
    for file in list_dir:
        if file.endswith(".csv"):
            df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file)
    
            df = df.select(df[0],df.close)
            
            df = df.withColumn("date", f.to_date(df[0]))
            
            df = df.groupBy(f.year("date").alias("Year"),f.month("date").alias("Month")).agg(f.avg("close").alias("close")).orderBy(f.col("Year"),f.col("Month"))#agrupamos por año y mes y hacemos el valor medio
            df= df.withColumn("n",f.lit(1))# esto es para usarlo en window

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





def top5HighestGrowth(bucket_name):

    list_dirUS= []
    list_dirIndia= []
    list_dirChina= []
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs_all = list(bucket.list_blobs())
    for blob in blobs_all:  
        file = f"gs://{bucket_name}/{blob.name}"
        if blob.name.endswith('.csv'):
            if blob.name.startswith('US') :
                list_dirUS.append(file)
            elif blob.name.startswith('china'):
                list_dirChina.append(file)
            elif blob.name.startswith('india'):
                list_dirIndia.append(file)        
    result=[]
    result.extend(top5HighestGrowthUS(list_dirUS))
    result.extend(top5HighestGrowthChina(list_dirChina))
    result.extend(top5HighestGrowthIndia(list_dirIndia))
    

    result = sorted(result, key=lambda x : x[1], reverse=True)

    return result[:5]


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit top5HighestGrowth.py <bucket_name>")
    print(top5HighestGrowth(sys.argv[1]))
