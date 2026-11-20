"""
Phần 2 - Tutorial trong đề Lab 02 (trang 4-5):
Đọc JSON từ TCP socket port 9999 (dùng lệnh: nc -l -p 9999),
join với bảng giá tĩnh để tính doanh thu theo từng loại trái cây và in ra console.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum, to_timestamp, window

spark = (
    SparkSession.builder
    .appName("Lab02-Tutorial-Socket")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Bước 3: Tạo bảng tĩnh (static DataFrame) chứa đơn giá từng món
df_static = spark.createDataFrame(
    [("apple", 2.0), ("banana", 1.0), ("orange", 1.5)],
    ["item", "price"],
)

# Bước 4: Đọc dữ liệu JSON từ socket localhost:9999 và tách cột
df = (
    spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
    .select(from_json(col("value"), "timestamp STRING, item STRING, amount INT").alias("data"))
    .select("data.*")
    .withColumn("timestamp", to_timestamp(col("timestamp")))
)

# Bước 5: Gom nhóm tính tổng số lượng theo từng mode ("complete", "append", "update")
MODE = "update"

if MODE == "append":
    df_stream = (
        df.withWatermark("timestamp", "1 seconds")
        .groupBy(window(col("timestamp"), "10 seconds", "5 seconds"), col("item"))
        .agg(sum("amount").alias("total_amount"))
        .select("item", "total_amount")
    )
else:
    df_stream = (
        df.groupBy(col("item"))
        .agg(sum("amount").alias("total_amount"))
    )

# Bước 6: Join với bảng giá tĩnh để tính doanh thu
df_final = (
    df_stream.join(df_static, on="item", how="left")
    .withColumn("revenue", col("total_amount") * col("price"))
    .drop("price")
)

# Bước 7: In kết quả ra console
query = (
    df_final.writeStream
    .outputMode(MODE)
    .format("console")
    .option("truncate", "false")
    .start()
)
query.awaitTermination()
