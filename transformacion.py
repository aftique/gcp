
gcloud dataproc clusters create dataproc-demo \
                --bucket pruebas-trans \
                --region us-central1 \
                --zone us-central1-a \
                --master-machine-type n1-standard-2 \
                --master-boot-disk-size 100 \
                --num-workers 2 \
                --worker-machine-type n1-standard-2 \
                --worker-boot-disk-size 50 \
                --image-version 2.0-debian10 \
                --scopes 'https://www.googleapis.com/auth/cloud-platform' \
                --tags hadoopdemo --project ambiente-pruebas-353301 \
                --initialization-actions gs://goog-dataproc-initialization-actions-us-central1/connectors/connectors.sh \
                --metadata bigquery-connector-version=1.2.0 \
                --metadata spark-bigquery-connector-version=0.21.0



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