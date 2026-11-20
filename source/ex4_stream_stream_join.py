"""
Exercise 4: Stream-Stream Join: Tags and Ratings
- Đặt watermark ở cả 2 luồng ratings và tags
- Stream-stream join trên movieId ở append mode (không dùng aggregation)
- Xuất ra console 5 cột (movieId, tag, rating, ratingTime, tagTime) mỗi 5 giây và lưu output/ex4_stream_stream_join.csv
"""

import os
import time
from config import OUTPUT_DIR
from ex1_prepare_data import create_spark, read_ratings_stream, read_tags_stream, save_to_csv

if __name__ == "__main__":
    spark = create_spark("Lab02-Ex4-StreamStreamJoin")

    # Chuẩn bị luồng ratings kèm watermark
    ratings_stream = (
        read_ratings_stream(spark)
        .withColumnRenamed("timestamp", "ratingTime")
        .withWatermark("ratingTime", "10 minutes")
        .select("movieId", "rating", "ratingTime")
    )

    # Chuẩn bị luồng tags kèm watermark
    tags_stream = (
        read_tags_stream(spark)
        .withColumnRenamed("timestamp", "tagTime")
        .withWatermark("tagTime", "10 minutes")
        .select("movieId", "tag", "tagTime")
    )

    # Stream-stream join theo movieId (inner join, append mode)
    joined_stream = (
        ratings_stream
        .join(tags_stream, on="movieId", how="inner")
        .select("movieId", "tag", "rating", "ratingTime", "tagTime")
    )

    # Xóa file CSV cũ nếu có để lần chạy mới không bị nối trùng dữ liệu cũ
    out_file = os.path.join(OUTPUT_DIR, "ex4_stream_stream_join.csv")
    if os.path.exists(out_file):
        os.remove(out_file)

    def process_batch(batch_df, batch_id):
        batch_df.persist()
        try:
            print("-------------------------------------------", flush=True)
            print(f"Batch: {batch_id}", flush=True)
            print("-------------------------------------------", flush=True)
            save_to_csv(batch_df, "ex4_stream_stream_join.csv", mode="append")
            batch_df.show(20, truncate=False)
        finally:
            batch_df.unpersist()

    query = (
        joined_stream.writeStream
        .outputMode("append")
        .foreachBatch(process_batch)
        .trigger(processingTime="5 seconds")
        .start()
    )

    # Chờ batch đầu tiên chạy xong rồi in query plan để kiểm chứng watermark xuất hiện ở cả 2 phía join
    while query.isActive and query.lastProgress is None:
        time.sleep(0.5)
    print("=== Kiểm tra Physical Plan (Watermark ở cả 2 nhánh) ===", flush=True)
    query.explain()

    query.awaitTermination()
