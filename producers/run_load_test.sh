#!/bin/bash

# =====================================================================
# SCRIPT KHỞI CHẠY ĐA TIẾN TRÌNH KAFKA PRODUCER (PHASE 4 - LOAD TEST)
# =====================================================================

# 1. CẤU HÌNH THAM SỐ ĐỂ THAY ĐỔI THEO TỪNG BƯỚC TEST
NUM_PROCESSES=2      # Số tiến trình chạy song song (Bước 1: Test 1, 2, 4, 6, 8)
LINGER_MS=50         # Thời gian chờ gom mẻ (Bước 2: Test 0, 10, 50)
BATCH_SIZE_KB=16     # Kích thước cụm tính bằng KB (Bước 2: Test 16, 64, 128)
ACKS="1"             # Cơ chế xác thực: 0 (tốc độ), 1 (mặc định), hoặc all (an toàn)

echo "========================================================="
echo "KÍCH HOẠT HỆ THỐNG ĐẨY TẢI ĐA TIẾN TRÌNH KAFKA PRODUCER"
echo "Số lượng tiến trình chạy song song: $NUM_PROCESSES"
echo "Cấu hình: linger_ms=$LINGER_MS | batch_size=${BATCH_SIZE_KB}KB | acks=$ACKS"
echo "========================================================="

# 2. VÒNG LẶP KHỞI CHẠY CÁC TIẾN TRÌNH CHẠY NGẦM (&)
for ((i=1; i<=NUM_PROCESSES; i++))
do
    echo "[+] Đang kích hoạt Tiến trình thứ #$i..."
    # Truyền trực tiếp các tham số cấu hình sang cho tệp Python nhận diện
    python3 realtime_producer.py $i $LINGER_MS $BATCH_SIZE_KB $ACKS &
done

echo "========================================================="
echo "[+] TẤT CẢ CÁC TIẾN TRÌNH ĐÃ ĐƯỢC KÍCH HOẠT CHẠY NGẦM."
echo "Để DỪNG hoàn toàn việc đẩy tải, hãy gõ lệnh: killall python3"
echo "========================================================="

# Giữ script luôn mở để thu thập log đầu ra
wait
