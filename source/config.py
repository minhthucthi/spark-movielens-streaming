import os

# Địa chỉ Kafka broker:
# - Khi chạy bằng Docker ở nhà: lấy tự động từ biến môi trường KAFKA_SERVERS
# - Khi chạy trực tiếp ngoài Docker: dùng localhost:9092
# - Khi lên phòng Lab trường demo: đổi giá trị mặc định thành "10.1.1.10:30090,10.1.1.27:30091,10.1.1.203:30092"
KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "localhost:9092")

# Tên 3 topic Kafka theo đúng đề bài Lab 2
TOPIC_MOVIES = "Lab1_movies"
TOPIC_RATINGS = "Lab1_ratings"
TOPIC_TAGS = "Lab1_tags"

# Thư mục lưu các file kết quả .csv (Proof of work)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
