"""
Exercise 3: What goes on the Trending Now rail?
- Đặt watermark trên ratings trước khi chia window
- Dùng tumbling window 5 phút trên eventTime, đếm theo (window, movieId)
- Join với movies (static) để lấy title
- Dùng dense_rank() theo partitionBy(window).orderBy(cnt desc, movieId asc) để lấy Top 3 mỗi window
- Xuất đúng 5 cột (window, movieId, title, cnt, rank) với complete mode, mỗi 5 giây
"""

import time
from pyspark.sql.functions import col, count, dense_rank, window
from pyspark.sql.window import Window

from ex1_prepare_data import create_spark, read_movies, read_ratings_stream, save_to_csv

if __name__ == "__main__":
    spark = create_spark("Lab02-Ex3-TrendingNow")

    movies_df = read_movies(spark)
    ratings_df = read_ratings_stream(spark).withColumnRenamed("timestamp", "eventTime")

    # Đặt watermark trên cột eventTime trước khi chia tumbling window 5 phút
    windowed_counts = (
        ratings_df
        .withWatermark("eventTime", "10 minutes")
        .groupBy(window(col("eventTime"), "5 minutes"), col("movieId"))
        .agg(count("*").alias("cnt"))
    )

    # Join với bảng movies tĩnh để lấy tên phim (title)
    joined_counts = windowed_counts.join(
        movies_df.select("movieId", "title"),
        on="movieId",
        how="inner",
    )

    # Spark Streaming chưa hỗ trợ trực tiếp dense_rank trên streaming DataFrame,
    # nên mình xử lý bước xếp hạng Top 3 ở trong foreachBatch với complete mode
    def process_batch(batch_df, batch_id):
        window_spec = Window.partitionBy("window").orderBy(
            col("cnt").desc(),
            col("movieId").asc(),  # tie-break: cùng lượt thì xếp movieId nhỏ hơn lên trước
        )

        ranked_df = batch_df.withColumn("rank", dense_rank().over(window_spec))

        top3_df = (
            ranked_df
            .filter(col("rank") <= 3)
            .select("window", "movieId", "title", "cnt", "rank")
            .orderBy(col("window.start").desc(), col("rank").asc())
        )

        print(f"-------------------------------------------", flush=True)
        print(f"Batch: {batch_id}", flush=True)
        print(f"-------------------------------------------", flush=True)
        top3_df.show(30, truncate=False)
        save_to_csv(top3_df, "ex3_trending_now.csv", mode="overwrite")

    query = (
        joined_counts.writeStream
        .outputMode("complete")
        .foreachBatch(process_batch)
        .trigger(processingTime="5 seconds")
        .start()
    )

    # Chờ batch đầu tiên chạy xong rồi in query plan để kiểm chứng có EventTimeWatermark trước window
    while query.isActive and query.lastProgress is None:
        time.sleep(0.5)
    print("=== Kiểm tra Physical Plan (có EventTimeWatermark trước Aggregate) ===", flush=True)
    query.explain()

    query.awaitTermination()
