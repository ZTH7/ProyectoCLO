# ANÁLISIS DE LOS VALORES DE LA BOLSA
***

## Descripción
***
​	El objetivo de este proyecto consiste en analizar los datos de la bolsa de valores de diferentes partes del mundo y con ello sacar conclusiones de las mejores acciones y crear estimaciones de los posibles crecimientos o decrecimientos de las acciones.

## Datasets
***
From Kaggle: [Stock US](https://www.kaggle.com/datasets/paultimothymooney/stock-market-data), [Stock China](https://www.kaggle.com/datasets/stevenchen116/stockchina-minute) y [Stock India](https://www.kaggle.com/datasets/debashis74017/stock-market-data-nifty-50-stocks-1-min-data)
Hay disponibles dos versiones más pequeñas de este conjunto de datos (1 MB cada una) cargadas en este repositorio (./Samples).

## Ejecuta PySpark por Máquina Local
***
### 1. Instalación de Python y pip
​	Para poder ejecutar los scripts de Python, es necesario tener instalado Python y un intérprete de comandos de tipo Unix. Este intérprete puede ser Linux, una máquina de Google Cloud o en usando el WSL de Windows. Python, por lo general, ya viene pre-instalado en el sistema. Para poder tener la versión más actualizada, es necesario ejecutar el siguiente comando:
```shell
$ sudo apt update
$ sudo apt install python3
$ sudo apt install python3-pip
```
### 2. Instalación de Java Runtime Enviorment(JRE)
```shell
$ sudo apt install default-jre
```
### 3. Instalación de PySpark
```shell
$ pip install pyspark
```
### 4. Ejecuta los Python Scripts
```shell
$ spark-submit <script>
```

## Ejecuta PySpark por Google Cloud
***
​	Deberá descargar todos los archivos del repositorio en local, crear un BUCKET en Google Cloud y subir todos los archivos allí. Después de subir todos los archivos. Crará el cluster con el siguiente comando en la terminal de Google Cloud:
```shell
$ gcloud dataproc clusters create example-cluster --region europe-west6 --enable-component-gateway --master-boot-disk-size 50GB --worker-boot-disk-size 50GB
```
Tras la creación del cluster, puede crear un Spark Job con el siguiente comando:

```shell
$ BUCKET=gs://<your bucket name>
$ gcloud dataproc jobs submit pyspark --cluster example-cluster --region=europe-west6 $BUCKET/Script/main.py -- $BUCKET/Samples $BUCKET/Result
```
