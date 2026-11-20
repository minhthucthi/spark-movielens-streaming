"""
Script chuẩn bị dữ liệu cho Lab 02 khi chạy Kafka local bằng Docker:
1. Xóa (nếu đã có) và tạo lại 3 topic: Lab1_movies, Lab1_ratings, Lab1_tags
2. Đọc 3 file CSV từ thư mục data/movielens/ rồi đẩy lên Kafka dưới dạng JSON
3. Kiểm tra lại số dòng trong Kafka khớp với số dòng trong CSV
"""

import os
import time
from confluent_kafka.admin import AdminClient, NewTopic
from pyspark.sql import SparkSession

from config import KAFKA_SERVERS, TOPIC_MOVIES, TOPIC_RATINGS, TOPIC_TAGS

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data", "movielens")

TOPICS = [
    (TOPIC_MOVIES, "movies.csv"),
    (TOPIC_RATINGS, "ratings.csv"),
    (TOPIC_TAGS, "tags.csv"),
]


def setup_topics():
    admin = AdminClient({"bootstrap.servers": KAFKA_SERVERS})
    topic_names = [t[0] for t in TOPICS]

    # Chờ Kafka sẵn sàng (phòng trường hợp mới bật docker compose xong)
    cluster_topics = None
    for _ in range(15):
        try:
            cluster_topics = admin.list_topics(timeout=5).topics
            break
        except Exception:
            print("Đang chờ Kafka khởi động...")
            time.sleep(2)

    if cluster_topics is None:
        raise RuntimeError(f"Không kết nối được tới Kafka tại {KAFKA_SERVERS}")

    # Xóa topic cũ nếu có để tránh dữ liệu bị đẩy trùng
    existing = [name for name in topic_names if name in cluster_topics]
    if existing:
        print("Đang xóa topic cũ:", existing)
        futures = admin.delete_topics(existing, operation_timeout=30)
        for name, future in futures.items():
            try:
                future.result()
            except Exception as err:
                print(f"Không xóa được topic {name}: {err}")

        # Chờ tới khi topic bị xóa hoàn toàn
        for _ in range(30):
            current = admin.list_topics(timeout=10).topics
            if not any(name in current for name in existing):
                break
            time.sleep(1)

    # Tạo lại 3 topic mới
    new_topics = [
        NewTopic(name, num_partitions=1, replication_factor=3)
        for name in topic_names
    ]
    futures = admin.create_topics(new_topics)
    for name, future in futures.items():
        future.result()
        print(f"Đã tạo topic: {name}")


def push_csv_to_kafka():
    spark = (
        SparkSession.builder
        .appName("Lab02-Setup-Kafka")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
        .config("spark.jars.ivy", "/tmp/.ivy2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Đọc từng file CSV và ghi vào topic Kafka dưới dạng JSON
    for topic_name, file_name in TOPICS:
        csv_path = os.path.join(DATA_DIR, file_name).replace("\\", "/")
        df = spark.read.csv(f"file://{csv_path}", header=True, inferSchema=True)

        (
            df.selectExpr("to_json(struct(*)) AS value")
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_SERVERS)
            .option("topic", topic_name)
            .save()
        )
        print(f"Đã đẩy file {file_name} lên topic {topic_name}")

    # Kiểm tra lại số lượng dòng
    print("\n--- Kiểm tra số dòng giữa CSV và Kafka ---")
    for topic_name, file_name in TOPICS:
        csv_path = os.path.join(DATA_DIR, file_name).replace("\\", "/")
        n_csv = spark.read.csv(f"file://{csv_path}", header=True).count()
        n_kafka = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_SERVERS)
            .option("subscribe", topic_name)
            .option("startingOffsets", "earliest")
            .load()
            .count()
        )
        trang_thai = "OK" if n_csv == n_kafka else "LECH!"
        print(f"{topic_name}: Kafka = {n_kafka} | CSV = {n_csv} -> {trang_thai}")

    spark.stop()


if __name__ == "__main__":
    print(f"Kết nối Kafka tại: {KAFKA_SERVERS}")
    setup_topics()
    push_csv_to_kafka()
