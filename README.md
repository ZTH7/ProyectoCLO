# ANÁLISIS DE LOS VALORES DE LA BOLSA

## Descripción
​	El objetivo de este proyecto consiste en analizar los datos de la bolsa de valores de diferentes partes del mundo y con ello sacar conclusiones de las mejores acciones y crear estimaciones de los posibles crecimientos o decrecimientos de las acciones.

## Datasets
  From Kaggle: [Stock US](https://www.kaggle.com/datasets/paultimothymooney/stock-market-data), [Stock China](https://www.kaggle.com/datasets/stevenchen116/stockchina-minute) y [Stock India](https://www.kaggle.com/datasets/debashis74017/stock-market-data-nifty-50-stocks-1-min-data)

  Hay disponibles dos versiones más pequeñas de este conjunto de datos (1 MB cada una) cargadas en este repositorio (./Samples).

## Ejecuta PySpark por Máquina Local
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
​	Deberá descargar todos los archivos del repositorio en local, crear un BUCKET en Google Cloud y subir todos los archivos allí. Después de subir todos los archivos. Crará el cluster con el siguiente comando en la terminal de Google Cloud:
```shell
$ gcloud dataproc clusters create example-cluster --region europe-west6 --enable-component-gateway --master-boot-disk-size 50GB --worker-boot-disk-size 50GB
```
  Tras la creación del cluster, puede crear un Spark Job con el siguiente comando:

```shell
$ BUCKET=gs://<your bucket name>
$ gcloud dataproc jobs submit pyspark --cluster example-cluster --region=europe-west6 $BUCKET/Script/main.py -- $BUCKET/Samples $BUCKET/Result
```

## Ejecución

​	Para ejecutar nuestros scripts de manera individual puede utilizarse el comando:

```
$ spark-submit <nombre_archivo.py> [argumentos]
```

  Sin embargo una opción más simple es utilizar el script *main.py* que contiene un menú con el que interactuar de forma más sencilla con los scripts. Esto se haría ejecutando:

```
$ python main.py
```

  Sino también se puede ejecutar con el siguiente comando:

```
$ python3 main.py
```

**Importante**:¨Este comando se debe ejecutar sin haber activado el ambiente de Python.

  Desde *main.py* los scripts se ejecutan en Spark en modo local y antes de mostrar las opciones se pide al usuario que introduzca el número de cores de su procesador que desea utilizar para la ejecución. Si el usuario desea ejecutar la aplicación en Google Cloud, más adelante se explica cómo hacerlo.

  Después de elegir el número de cores, se mostrará por pantalla un menú como éste:

```shell
Menú:
1. Ver 10 acciones más caras históricamente
2. Ver 10 acciones más caras en un determinado <year>
3. Ver 10 acciones más caras en un determinado <country> históricamente
4. Ver 10 acciones más caras en un determinado <country> en un determinado <year>
5. Ver 5 acciones con mayor crecimiento históricamente
6. Ver 5 acciones con mayor crecimiento en un determinado <year>
7. Ver 5 acciones con mayor crecimiento en un determinado <country> históricamente
8. Ver 5 acciones con mayor crecimiento en un determinado <country> en un determinado <year>
9. Ver cuál es la probabilidad de que una acción aumente de valor en un determinado <year>

Ingrese el número de la opción que desea:
```
