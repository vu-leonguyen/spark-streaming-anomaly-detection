import json
import time
import pandas as pd
import numpy as np
from kafka import KafkaProducer


KAFKA_TOPIC = "jsonproducerp3"
KAFKA_SERVER = "localhost:9092"
CSV_PATH = "../datasets/Dataset.csv" 
MESSAGES_PER_SECOND = 5000
TOTAL_MESSAGES = 200000

# Hàm xử lý lỗi kiểu dữ liệu Pandas/Numpy khi chuyển sang JSON
def json_serializer(obj):
    if isinstance(obj, (np.int64, np.int32)):
        return int(obj)
    if isinstance(obj, (np.float64, np.float32)):
        return float(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

# ==========================================
# LOAD DATASET
# ==========================================
try:
    df = pd.read_csv(CSV_PATH)
    # Chuyển DataFrame thành list các dictionary để lặp nhanh hơn iterrows
    data_records = df.to_dict('records')
    print(f"Dataset loaded: {len(df)} rows")
except Exception as e:
    print(f"Error loading CSV: {e}")
    exit()

# ==========================================
# CREATE PRODUCER
# ==========================================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v, default=json_serializer).encode("utf-8"),
    linger_ms=20, 
    batch_size=65536, 
    acks=1
)

# ==========================================
# SEND LOOP
# ==========================================
sleep_time = 1.0 / MESSAGES_PER_SECOND
count = 0

print("====================================")
print("STARTING REALTIME PRODUCER...")
print(f"Target: {TOTAL_MESSAGES} messages")
print("====================================")

try:
    # Vòng lặp chính cho đến khi đạt đủ số lượng tin nhắn
    while count < TOTAL_MESSAGES:
        for message in data_records:
            if count >= TOTAL_MESSAGES:
                break

            # Gửi tin nhắn
            producer.send(KAFKA_TOPIC, value=message)
            count += 1

            # Log trạng thái mỗi 100 tin nhắn
            if count % 1000 == 0:
                print(f"Sent: {count} messages")

            # Kiểm soát tốc độ gửi
            time.sleep(sleep_time)
            
        if count < TOTAL_MESSAGES:
            print("--- Reached end of CSV, restarting loop from beginning ---")

except KeyboardInterrupt:
    print("\n[STOP] KeyboardInterrupt detected. Stopping producer...")
except Exception as e:
    print(f"\n[ERROR] An error occurred: {e}")
finally:
    # Đảm bảo tất cả tin nhắn trong bộ đệm được gửi đi trước khi đóng
    print("Flushing and closing producer...")
    producer.flush()
    producer.close()
    print("Producer closed. Finished.")
