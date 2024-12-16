from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType

ip_hdfs = "172.22.0.6"
puerto_hdfs = "9000"
ip_kafka = "10.2.116.248"

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaJSONProcessor") \
    .config("spark.hadoop.fs.defaultFS", f"hdfs://{ip_hdfs}:{puerto_hdfs}") \
    .getOrCreate()

# Configurar checkpoint temporal
spark.conf.set("spark.sql.streaming.checkpointLocation", f"hdfs://{ip_hdfs}:{puerto_hdfs}/tmp/spark-checkpoints")

# Definir el esquema del JSON (ajusta según tus datos)
schema = StructType() \
    .add("latitud", StringType()) \
    .add("longitud", StringType()) \
    .add("region", StringType()) \
    .add("consumption_kWh", DoubleType()) \
    .add("timestamp", StringType())

# Leer del topic de Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{ip_kafka}:9092") \
    .option("subscribe", "duran-OUT,samborondon-OUT") \
    .load()

# Convertir los datos a JSON y aplicar el esquema
json_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), schema).alias("data")) \
    .select("data.*")

# Convertir `timestamp` a tipo TIMESTAMP
'''json_stream = json_stream.withColumn("timestamp", to_timestamp(col("timestamp")))

# Configurar watermark para manejar datos tardíos
json_stream_with_watermark = json_stream.withWatermark("timestamp", "10 minutes")

# Calcular el consumo promedio por región con watermark
average_consumption_stream = json_stream_with_watermark.groupBy("región").agg(avg("consumption_kWh").alias("consumo_promedio")) \
    .select(
        col("region"),
        col("consumo_promedio")
    )

# Escribir el consumo promedio por región en consola y guardar en un directorio local
query_avg_consumption = average_consumption_stream.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .option("truncate", "false") \
    .start()

query_avg_consumption_csv = average_consumption_stream.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "/opt/spark-output/average_consumption") \
    .option("checkpointLocation", "/opt/spark-output/checkpoints/average_consumption") \
    .start()
'''
# Escribir los datos recibidos en consola y guardar en un directorio local
query_data = json_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .option("truncate", "false") \
    .start()

query_data_csv = json_stream.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "/spark-output/data") \
    .option("checkpointLocation", "/spark-output/checkpoints/data") \
    .start()

# Escribir las anomalías detectadas en consola y guardar en un directorio local
anomalies_stream = json_stream.filter(col("consumption_kWh") >= 100) \
    .select(
        col("latitud"),
        col("longitud"),
        col("region"),
        col("timestamp"),
        col("consumption_kWh").alias("valor_pico")
    )

query_anomalies = anomalies_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .option("truncate", "false") \
    .start()

query_anomalies_csv = anomalies_stream.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "/spark-output/anomalies") \
    .option("checkpointLocation", "/spark-output/checkpoints/anomalies") \
    .start()

# Esperar a que los streams terminen
query_data.awaitTermination()
query_anomalies.awaitTermination()
#query_avg_consumption.awaitTermination()
query_data_csv.awaitTermination()
query_anomalies_csv.awaitTermination()
#query_avg_consumption_csv.awaitTermination()
