from pyspark.sql.functions import col
from pyspark.sql.functions import sum
from pyspark.sql import SparkSession
spark = SparkSession.builder \
  .appName('Jupyter BigQuery Storage')\
  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar') \
  .getOrCreate()


bucket = 'transformaciones-dav'
spark.conf.set('temporaryGcsBucket', bucket)


df= spark.read.option('header', True).csv('gs://transformaciones-dav/test.csv')

req_df = df.select(col('SEGMENTO'),col('FECHA_CARGUE'))

req_df.printSchema()

req_df.write.format('bigquery') \
.option('table', 'dataset_demo_8096.agg_output').option('createDisposition','CREATE_IF_NEEDED') \
.save()
spark.stop()
