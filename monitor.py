import psutil
import os
import time
import csv
import sys

def list_processes():
    print(f"{'PID':<10} | {'Type':<10} | {'Command Line'}")
    print("-" * 70)
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            pinfo = proc.info
            name = pinfo['name'].lower()
            if 'java' in name or 'python' in name:
                cmd = " ".join(pinfo['cmdline']) if pinfo['cmdline'] else ""
                if pinfo['pid'] != os.getpid():
                    p_type = "JAVA/SPARK" if 'java' in name else "PYTHON"
                    print(f"{pinfo['pid']:<10} | {p_type:<10} | {cmd[:100]}...")
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

print("====================================================")
print("HỆ THỐNG GIÁM SÁT BASELINE WATERLOG")
print("====================================================")
list_processes()

try:
    target_pid = int(input("\nNhập PID muốn giám sát: "))
    process = psutil.Process(target_pid)
    num_cores = psutil.cpu_count() # Lấy số nhân (4)
    print(f"\n[START] Giám sát PID: {target_pid} ({num_cores} cores detected)")
except (ValueError, psutil.NoSuchProcess):
    print("[ERROR] PID không hợp lệ.")
    sys.exit()

log_file = "model_resource_metrics.csv"

with open(log_file, "w", newline="") as f:
    writer = csv.writer(f)
    # Thêm cột % CPU tổng để dễ làm báo cáo
    writer.writerow(["timestamp", "process_cpu_percent", "system_relative_cpu", "ram_rss_mb", "ram_vms_mb"])

    try:
        # Gọi lần đầu để khởi tạo mốc đo CPU
        process.cpu_percent(interval=None)
        
        while True:
            # interval=1.0 là bắt buộc để psutil tính toán chính xác delta CPU
            cpu_p = process.cpu_percent(interval=1.0)
            
            # CPU tương đối trên toàn hệ thống (0-100%)
            sys_cpu = cpu_p / num_cores
            
            mem = process.memory_info()
            rss_mb = mem.rss / (1024 * 1024) # RAM vật lý thực tế
            vms_mb = mem.vms / (1024 * 1024) # RAM ảo (Sẽ thấy ~2000+ MB cho Baseline)
            
            writer.writerow([time.time(), cpu_p, sys_cpu, rss_mb, vms_mb])
            f.flush()

            print(f"CPU (Core): {cpu_p:6.1f}% | CPU (Sys): {sys_cpu:5.1f}% | RAM RSS: {rss_mb:7.1f} MB | VMS: {vms_mb:7.1f} MB")
            
    except KeyboardInterrupt:
        print(f"\n[STOP] Dữ liệu đã lưu vào {log_file}")
    except psutil.NoSuchProcess:
        print("\n[FINISHED] Tiến trình mục tiêu đã đóng.")
