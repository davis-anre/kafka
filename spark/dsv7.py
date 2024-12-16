from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StringType, DoubleType

# Definir las variables
ip_hdfs = "172.22.0.6"
puerto_hdfs = "9000"
ip_kafka = "192.168.0.100"

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

# Detectar anomalías en consumo_kWh
anomalies_stream = json_stream.filter(col("consumption_kWh") >= 100) \
    .select(
        col("latitud"),
        col("longitud"),
        col("region"),
        col("timestamp"),
        col("consumption_kWh").alias("valor_pico")
    )

# Formatear la salida para consola
formatted_data_stream = json_stream.select(
    col("latitud"),
    col("longitud"),
    col("region"),
    col("consumption_kWh"),
    col("timestamp")
)

# Escribir los datos recibidos en consola y HDFS
query_data = formatted_data_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .option("truncate", "false") \
    .start()

query_data_hdfs = formatted_data_stream.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", f"hdfs://{ip_hdfs}:{puerto_hdfs}/data/received") \
    .option("checkpointLocation", f"hdfs://{ip_hdfs}:{puerto_hdfs}/tmp/spark-checkpoints/data_received") \
    .start()

# Escribir las anomalías detectadas en consola y HDFS
query_anomalies = anomalies_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .option("truncate", "false") \
    .start()

query_anomalies_hdfs = anomalies_stream.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", f"hdfs://{ip_hdfs}:{puerto_hdfs}/data/anomalies") \
    .option("checkpointLocation", f"hdfs://{ip_hdfs}:{puerto_hdfs}/tmp/spark-checkpoints/data_anomalies") \
    .start()

# Esperar a que los streams terminen
query_data.awaitTermination()
query_anomalies.awaitTermination()
query_data_hdfs.awaitTermination()
query_anomalies_hdfs.awaitTermination()
