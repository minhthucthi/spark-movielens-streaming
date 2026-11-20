# Big Data Lab 02 (CO3137) - Spark Structured Streaming

## Cấu trúc thư mục

```text
spark-movielens-streaming/
├── Dockerfile                     # Image Spark 4.0.1 + confluent-kafka + jupyterlab
├── docker-compose.yml             # Cụm 3 Kafka broker + 1 container Spark (local[*])
├── data/movielens/                # Bộ dữ liệu MovieLens (movies.csv, ratings.csv, tags.csv)
├── source/
│   ├── config.py                  # Cấu hình địa chỉ Kafka & tên 3 topic
│   ├── setup_kafka.py             # Đẩy CSV vào Kafka local (Lab1_movies, Lab1_ratings, Lab1_tags)
│   ├── tutorial_socket.py         # Phần 2 Tutorial: đọc JSON từ socket 9999
│   ├── ex1_prepare_data.py        # Exercise 1: đọc 3 topic Kafka và ép kiểu schema
│   ├── ex2_hot_genres.py          # Exercise 2: static-stream join + thống kê thể loại phim
│   ├── ex3_trending_now.py        # Exercise 3: watermark + window 5 phút + Top 3 phim
│   ├── ex4_stream_stream_join.py  # Exercise 4: stream-stream join giữa ratings và tags
│   └── lab02_notebook.ipynb       # Notebook tổng hợp để chạy trên JupyterLab
├── output/                        # Thư mục lưu sẵn các file .csv kết quả (Proof of work)
└── README.md                      # Hướng dẫn sử dụng
```

## Cách chạy local bằng Docker

Mở PowerShell tại thư mục `lab02\spark-movielens-streaming`:

### Bước 1: Khởi động Kafka và Spark
```powershell
docker compose up -d --build
```
*(Chờ khoảng 10-15 giây để 3 broker Kafka khởi động xong).*

### Bước 2: Tạo 3 topic và nạp dữ liệu MovieLens vào Kafka
```powershell
docker compose exec spark python -u source/setup_kafka.py
```
Kết quả đạt yêu cầu khi cả 3 dòng đều báo `OK`:
- `Lab1_movies`: 9742 dòng
- `Lab1_ratings`: 100836 dòng
- `Lab1_tags`: 3683 dòng

### Bước 3: Chạy từng bài tập
Các bài streaming chạy liên tục và in kết quả mỗi 5 giây. Nhấn `Ctrl + C` khi muốn dừng để chuyển sang bài tiếp theo:

```powershell
# Exercise 1: Kiểm tra schema và luồng đọc ratings (100 dòng/trigger)
docker compose exec spark python -u source/ex1_prepare_data.py

# Exercise 2: Thể loại phim đang hot
docker compose exec spark python -u source/ex2_hot_genres.py

# Exercise 3: Top 3 phim Trending Now theo từng cửa sổ 5 phút
docker compose exec spark python -u source/ex3_trending_now.py

# Exercise 4: Stream-stream join giữa ratings và tags
docker compose exec spark python -u source/ex4_stream_stream_join.py
```

Hoặc mở trình duyệt tại `http://localhost:8888`, vào thư mục `source/` và mở file `lab02_notebook.ipynb` để chạy từng ô trực quan.

### Bước 4: Tắt container khi hoàn tất
```powershell
# Tắt nhưng giữ lại dữ liệu Kafka (lần sau bật lại bằng: docker compose start)
docker compose stop

# Hoặc tắt và xóa sạch container + dữ liệu Kafka
docker compose down -v
```
