"""
Exercise 2: Which genres are hot right now?
- Static-stream join giữa ratings (stream) và movies (static) theo movieId
- Tách chuỗi genres (dạng "Action|Adventure") thành từng thể loại rồi đếm số lượt đánh giá
- Xuất ra console mỗi 5 giây và lưu file output/ex2_hot_genres.csv
"""

from pyspark.sql.functions import avg, col, count, explode, round, split

from ex1_prepare_data import create_spark, read_movies, read_ratings_stream, save_to_csv

if __name__ == "__main__":
    spark = create_spark("Lab02-Ex2-HotGenres")

    # Đọc dữ liệu đã chuẩn bị từ Exercise 1
    movies_df = read_movies(spark)
    ratings_df = read_ratings_stream(spark)

    # Static-stream join để lấy cột genres cho mỗi lượt rating
    joined_df = ratings_df.join(movies_df.select("movieId", "genres"), on="movieId", how="inner")

    # Tách cột genres (ngăn cách bởi dấu |) thành nhiều dòng, mỗi dòng 1 thể loại
    exploded_df = (
        joined_df
        .withColumn("genre", explode(split(col("genres"), "\\|")))
        .filter((col("genre") != "") & (col("genre") != "(no genres listed)"))
    )

    # Gom nhóm theo thể loại để đếm lượt đánh giá và tính điểm trung bình
    hot_genres_df = (
        exploded_df
        .groupBy("genre")
        .agg(
            count("*").alias("rating_count"),
            round(avg("rating"), 2).alias("avg_rating"),
        )
    )

    def process_batch(batch_df, batch_id):
        ordered_df = batch_df.orderBy(col("rating_count").desc())
        print("-------------------------------------------", flush=True)
        print(f"Batch: {batch_id}", flush=True)
        print("-------------------------------------------", flush=True)
        ordered_df.show(truncate=False)
        save_to_csv(ordered_df, "ex2_hot_genres.csv", mode="overwrite")

    # Ghi kết quả ra console và lưu CSV mỗi 5 giây
    query = (
        hot_genres_df.writeStream
        .outputMode("complete")
        .foreachBatch(process_batch)
        .trigger(processingTime="5 seconds")
        .start()
    )
    query.awaitTermination()
