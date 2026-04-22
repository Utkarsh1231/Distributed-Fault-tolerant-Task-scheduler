import socket 
import time 
import threading 
import random 
import sys 
 
HOST = "192.168.0.103"  # Master IP 
PORT = 5000         # Master port 
HEARTBEAT_INTERVAL = 2  # seconds 
FAILURE_PROBABILITY = 0.4  # 20% chance to randomly crash 
 
def send_heartbeat(sock, worker_id): 
    while True: 
        try: 
            time.sleep(HEARTBEAT_INTERVAL) 
49 
 
            sock.sendall("HEARTBEAT".encode()) 
            print(f"[Worker {worker_id}] Sent heartbeat ") 
        except: 
            print(f"[Worker {worker_id}] Heartbeat failed, master unreachable ") 
            break 
 
def main(): 
    worker_id = random.randint(1000, 9999) 
    print(f"[Worker {worker_id}] Starting...") 
 
    try: 
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        sock.connect((HOST, PORT)) 
    except: 
        print(f"[Worker {worker_id}] Could not connect to master ") 
        return 
 
    sock.sendall(f"REGISTER {worker_id}".encode()) 
    print(f"[Worker {worker_id}] Connected to master ") 
 
    # Start heartbeat thread 
    heartbeat_thread = threading.Thread(target=send_heartbeat, args=(sock, worker_id)) 
    heartbeat_thread.daemon = True 
    heartbeat_thread.start() 
 
    while True: 
        try: 
            task = sock.recv(1024).decode() 
            if not task: 
                print(f"[Worker {worker_id}] Master disconnected ") 
                break 
 
            if task.startswith("TASK"): 
                _, task_id, sleep_time = task.split() 
                sleep_time = int(sleep_time) 
                print(f"[Worker {worker_id}] Received TASK {task_id}, working for {sleep_time}s ") 
 
                # Random failure simulation 
                if random.random() < FAILURE_PROBABILITY: 
                    print(f"[Worker {worker_id}] Simulating crash during TASK {task_id} ") 
                    sys.exit(1) 
 
                # Simulate work 
                time.sleep(sleep_time) 
                result = f"RESULT {task_id} done" 
                sock.sendall(result.encode()) 
                print(f"[Worker {worker_id}] Completed TASK {task_id} ") 
 
50 
 
        except Exception as e: 
            print(f"[Worker {worker_id}] Error: {e}") 
            break 
 
    sock.close() 
 
if __name__ == "__main__": 
    main()
