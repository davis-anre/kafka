from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StringType, DoubleType

# Crear la sesión de Spark
# Reemplazar IP con la del PC
spark = SparkSession.builder \
    .appName("KafkaJSONProcessor") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.0.100:9000") \
    .getOrCreate()

# Definir el esquema del JSON (ajusta según tus datos)
schema = StructType() \
    .add("latitud", StringType()) \
    .add("longitud", StringType()) \
    .add("region", StringType()) \
    .add("consumption_kWh", DoubleType()) \
    .add("timestamp", StringType())

# Leer del topic de Kafka
# Reemplazar IP con la del PC
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.0.100:9092") \
    .option("subscribe", "duran-OUT,samborondon-OUT") \
    .load()

# Convertir los datos a JSON y aplicar el esquema
json_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), schema).alias("data")) \
    .select("data.*")  # Expandir las columnas del JSON

# Calcular el promedio de consumo por región
avg_consumption_stream = json_stream.groupBy("region") \
    .agg(avg("consumption_kWh").alias("consumo_promedio"))

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
formatted_avg_stream = avg_consumption_stream.select(
    col("region").alias("Región"),
    col("consumo_promedio").alias("Consumo_promedio")
)

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

formatted_data_stream.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://192.168.0.100:9000/data/received") \
    .option("checkpointLocation", "hdfs://192.168.0.100:9000/checkpoints/received") \
    .start()

# Escribir el promedio por región en consola y HDFS
query_avg = formatted_avg_stream.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .option("truncate", "false") \
    .start()

avg_consumption_stream.writeStream \
    .outputMode("complete") \
    .format("parquet") \
    .option("path", "hdfs://192.168.0.100:9000/data/average") \
    .option("checkpointLocation", "hdfs://192.168.0.100:9000/checkpoints/average") \
    .start()

# Escribir las anomalías detectadas en consola y HDFS
query_anomalies = anomalies_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .option("truncate", "false") \
    .start()

anomalies_stream.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://192.168.0.100:9000/data/anomalies") \
    .option("checkpointLocation", "hdfs://192.168.0.100:9000/checkpoints/anomalies") \
    .start()

# Esperar a que los streams terminen
query_data.awaitTermination()
query_avg.awaitTermination()
query_anomalies.awaitTermination()
