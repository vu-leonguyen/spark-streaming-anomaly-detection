import sys
import json
import time
import pandas as pd
import numpy as np
from kafka import KafkaProducer

# ==========================================
# CẤU HÌNH THAM SỐ DÒNG LỆNH (PHASE 4)
# ==========================================
# Định dạng chạy: python3 realtime_producer.py [process_id] [linger_ms] [batch_size_kb] [acks]
if len(sys.argv) >= 5:
    PROCESS_ID = sys.argv[1]
    PARAM_LINGER_MS = int(sys.argv[2])
    PARAM_BATCH_SIZE = int(sys.argv[3]) * 1024  # Chuyển đổi từ KB sang Bytes
    # Đối với thư viện kafka-python, acks có thể nhận: 0, 1, hoặc 'all'
    raw_acks = sys.argv[4]
    PARAM_ACKS = 'all' if raw_acks == 'all' or raw_acks == '-1' else int(raw_acks)
else:
    # Cấu hình mặc định nếu chạy độc lập không qua script Bash
    PROCESS_ID = "1"
    PARAM_LINGER_MS = 20
    PARAM_BATCH_SIZE = 65536
    PARAM_ACKS = 1

KAFKA_TOPIC = "jsonproducerp3"
KAFKA_SERVER = "localhost:9092"
CSV_PATH = "../datasets/Dataset.csv" 
MESSAGES_PER_SECOND = 5000
TOTAL_MESSAGES = 100000

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
    data_records = df.to_dict('records')
    print(f"[Tiến trình #{PROCESS_ID}] Dataset loaded: {len(df)} rows")
except Exception as e:
    print(f"[Tiến trình #{PROCESS_ID}] Error loading CSV: {e}")
    sys.exit(1)

# ==========================================
# CREATE PRODUCER (Cấu hình tối ưu hóa)
# ==========================================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v, default=json_serializer).encode("utf-8"),
    linger_ms=PARAM_LINGER_MS, 
    batch_size=PARAM_BATCH_SIZE, 
    acks=PARAM_ACKS
)

# ==========================================
# SEND LOOP
# ==========================================
sleep_time = 1.0 / MESSAGES_PER_SECOND
count = 0

print("====================================")
print(f"STARTING PRODUCER #{PROCESS_ID}...")
print(f"Config: linger_ms={PARAM_LINGER_MS}, batch_size={PARAM_BATCH_SIZE}B, acks={PARAM_ACKS}")
print("====================================")

try:
    while count < TOTAL_MESSAGES:
        for message in data_records:
            if count >= TOTAL_MESSAGES:
                break

            # Gửi tin nhắn
            producer.send(KAFKA_TOPIC, value=message)
            count += 1

            # Log trạng thái mỗi 1000 tin nhắn
            if count % 1000 == 0:
                print(f"[Tiến trình #{PROCESS_ID}] Sent: {count} messages")

            # Kiểm soát tốc độ gửi theo nhịp
            time.sleep(sleep_time)
            
        if count < TOTAL_MESSAGES:
            print(f"[Tiến trình #{PROCESS_ID}] --- Reached end of CSV, restarting loop ---")

except KeyboardInterrupt:
    print(f"\n[Tiến trình #{PROCESS_ID} STOP] KeyboardInterrupt detected.")
except Exception as e:
    print(f"\n[Tiến trình #{PROCESS_ID} ERROR] An error occurred: {e}")
finally:
    print(f"[Tiến trình #{PROCESS_ID}] Flushing and closing producer...")
    producer.flush()
    producer.close()
    print(f"[Tiến trình #{PROCESS_ID}] Producer finished.")
