from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Spark Session
spark = SparkSession.builder \
    .appName("KafkaRealTimeETL") \
    .master("local[2]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Define schema for JSON messages
schema = StructType([
    StructField("orderId", StringType(), True),
    StructField("orderDate", StringType(), True),
    StructField("customer", StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])),
    StructField("product", StructType([
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", IntegerType(), True)
    ])),
    StructField("quantity", IntegerType(), True)
])

# 3. Read stream from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "order-topic") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Convert value from bytes to string and parse JSON
df_parsed = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# 5. Transform example: extract fields
etl_df = df_parsed.select(
    col("orderId"),
    col("orderDate"),
    col("customer.name").alias("customerName"),
    col("product.name").alias("productName"),
    col("product.price").alias("price"),
    col("quantity")
)

# 6. Write stream to console (can also write to DB, HDFS, etc.)
query = etl_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


# 🧩 4. Parse JSON payload
orders_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        col("data.orderId"),
        col("data.orderDate"),
        col("data.customer.name").alias("customer_name"),
        col("data.customer.age").alias("customer_age"),
        col("data.product.name").alias("product_name"),
        col("data.product.category").alias("product_category"),
        col("data.product.price").alias("product_price"),
        col("data.quantity")
    )
# 🧩 5. MySQL connection settings
# MYSQL_URL = "jdbc:mysql://localhost:3306/data_warehouse"
# MYSQL_TABLE = "orders"
# MYSQL_PROPERTIES = {
#     "user": "root",
#     "password": "1234",
#     "driver": "com.mysql.cj.jdbc.Driver"
# }

# # 🧩 6. Write each micro-batch to MySQL
# def write_to_mysql(batch_df, batch_id):
#     batch_df.write \
#         .jdbc(url=MYSQL_URL, table=MYSQL_TABLE, mode="append", properties=MYSQL_PROPERTIES)
#     print(f"✅ Batch {batch_id} written to MySQL successfully!")

# query = orders_df.writeStream \
#     .outputMode("append") \
#     .foreachBatch(write_to_mysql) \
#     .start()

query.awaitTermination()