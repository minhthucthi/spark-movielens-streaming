"""
Exercise 1: Prepare movie data (Bắt buộc)
- Đọc 3 topic Kafka: Lab1_movies (tĩnh), Lab1_ratings (stream 100 dòng/trigger), Lab1_tags (stream 100 dòng/trigger)
- Định nghĩa schema cho cả 3 bảng và chuyển đổi sang đúng kiểu dữ liệu tương ứng
"""

import csv
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, to_timestamp
from pyspark.sql.types import StringType, StructField, StructType

from config import KAFKA_SERVERS, OUTPUT_DIR, TOPIC_MOVIES, TOPIC_RATINGS, TOPIC_TAGS


def create_spark(app_name="Lab02-Spark"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
        .config("spark.jars.ivy", "/tmp/.ivy2")
        .config("spark.sql.shuffle.partitions", "4")
        # Tắt ANSI mode của Spark 4 để ép kiểu dữ liệu lỗi sẽ trả về NULL chứ không làm chết stream
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def save_to_csv(df, filename, mode="overwrite"):
    # Tạo thư mục output nếu chưa có rồi lưu kết quả về file .csv trên máy
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, filename)

    rows = df.collect()
    if not rows and mode == "append":
        return

    file_exists = os.path.exists(filepath)
    write_header = (mode != "append") or (not file_exists)
    open_mode = "a" if (mode == "append" and file_exists) else "w"

    with open(filepath, open_mode, newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(df.columns)
        for row in rows:
            line = []
            for col_name in df.columns:
                val = row[col_name]
                if col_name == "window" and val is not None:
                    line.append(f"{{{val['start']}, {val['end']}}}")
                else:
                    line.append(val)
            writer.writerow(line)


# Định nghĩa schema ban đầu dạng String để parse JSON an toàn (tránh mất cả dòng khi có giá trị sai kiểu)
movies_json_schema = StructType([
    StructField("movieId", StringType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True),
])

ratings_json_schema = StructType([
    StructField("userId", StringType(), True),
    StructField("movieId", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("timestamp", StringType(), True),
])

tags_json_schema = StructType([
    StructField("userId", StringType(), True),
    StructField("movieId", StringType(), True),
    StructField("tag", StringType(), True),
    StructField("timestamp", StringType(), True),
])


def read_movies(spark):
    # Đọc danh sách phim dưới dạng bảng tĩnh (static DataFrame)
    raw_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC_MOVIES)
        .option("startingOffsets", "earliest")
        .load()
    )

    movies_df = (
        raw_df
        .select(from_json(col("value").cast("string"), movies_json_schema).alias("data"))
        .select("data.*")
        # Ép kiểu movieId sang số nguyên (qua double để nhận cả chuỗi kiểu "1.0")
        .withColumn("movieId", col("movieId").cast("double").cast("int"))
        .filter(col("movieId").isNotNull() & col("title").isNotNull())
        .dropDuplicates(["movieId"])
    )
    return movies_df


def read_ratings_stream(spark):
    # Đọc ratings dạng stream với tốc độ 100 dòng mỗi trigger
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC_RATINGS)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 100)
        .load()
    )

    ratings_df = (
        raw_stream
        .select(from_json(col("value").cast("string"), ratings_json_schema).alias("data"))
        .select("data.*")
        # Chuyển đổi sang đúng kiểu dữ liệu của schema
        .withColumn("userId", col("userId").cast("double").cast("int"))
        .withColumn("movieId", col("movieId").cast("double").cast("int"))
        .withColumn("rating", col("rating").cast("double"))
        .withColumn("timestamp", to_timestamp(from_unixtime(col("timestamp").cast("double").cast("long"))))
        .filter(
            col("userId").isNotNull()
            & col("movieId").isNotNull()
            & col("rating").isNotNull()
            & col("timestamp").isNotNull()
            & (col("rating") >= 0.5)
            & (col("rating") <= 5.0)
        )
    )
    return ratings_df


def read_tags_stream(spark):
    # Đọc tags dạng stream tương tự ratings (100 dòng mỗi trigger)
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC_TAGS)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 100)
        .load()
    )

    tags_df = (
        raw_stream
        .select(from_json(col("value").cast("string"), tags_json_schema).alias("data"))
        .select("data.*")
        # Chuyển đổi sang đúng kiểu dữ liệu của schema
        .withColumn("userId", col("userId").cast("double").cast("int"))
        .withColumn("movieId", col("movieId").cast("double").cast("int"))
        .withColumn("timestamp", to_timestamp(from_unixtime(col("timestamp").cast("double").cast("long"))))
        .filter(
            col("userId").isNotNull()
            & col("movieId").isNotNull()
            & col("tag").isNotNull()
            & col("timestamp").isNotNull()
        )
    )
    return tags_df


if __name__ == "__main__":
    spark = create_spark("Lab02-Ex1-PrepareData")

    movies_df = read_movies(spark)
    ratings_df = read_ratings_stream(spark)
    tags_df = read_tags_stream(spark)

    print("=== Schema của movies (static) ===")
    movies_df.printSchema()
    movies_df.show(5, truncate=False)
    save_to_csv(movies_df.limit(50), "ex1_movies_sample.csv", mode="overwrite")

    print("=== Schema của ratings (streaming) ===")
    ratings_df.printSchema()

    print("=== Schema của tags (streaming) ===")
    tags_df.printSchema()

    # Xóa file CSV cũ của stream nếu có để lần chạy mới không bị ghi nối tiếp trùng lặp
    for old_file in ["ex1_ratings_stream.csv", "ex1_tags_stream.csv"]:
        old_path = os.path.join(OUTPUT_DIR, old_file)
        if os.path.exists(old_path):
            os.remove(old_path)

    def save_ratings_batch(batch_df, batch_id):
        print("-------------------------------------------", flush=True)
        print(f"Batch: {batch_id}", flush=True)
        print("-------------------------------------------", flush=True)
        batch_df.show(20, truncate=False)
        save_to_csv(batch_df, "ex1_ratings_stream.csv", mode="append")

    def save_tags_batch(batch_df, batch_id):
        save_to_csv(batch_df, "ex1_tags_stream.csv", mode="append")

    print("Đang chạy thử stream ratings & tags và lưu CSV vào output/ (Ctrl+C để dừng)...")
    query_ratings = (
        ratings_df.writeStream
        .outputMode("append")
        .foreachBatch(save_ratings_batch)
        .trigger(processingTime="5 seconds")
        .start()
    )
    query_tags = (
        tags_df.writeStream
        .outputMode("append")
        .foreachBatch(save_tags_batch)
        .trigger(processingTime="5 seconds")
        .start()
    )
    query_ratings.awaitTermination()
