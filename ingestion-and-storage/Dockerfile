FROM apache/spark:3.5.1-python3

# Chuyển sang root để cài đặt gói
USER root

# Cài đặt các gói Python cần thiết
RUN pip install --no-cache-dir kafka-python python-dotenv

# Chuyển lại người dùng mặc định của image
USER spark