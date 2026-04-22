import socket 
import threading 
import pickle 
import time 
import queue 
 
MASTER_HOST = "0.0.0.0" 
MASTER_PORT = 5000 
 
workers = {}  # socket per worker 
worker_last_heartbeat = {}  # track last heartbeat 
worker_tasks = {}  # track tasks assigned per worker 
tasks_queue = queue.Queue() 
 
HEARTBEAT_TIMEOUT = 6 
 
def client_handler(conn, addr): 
    try: 
        worker_id = conn.recv(1024).decode() 
        workers[worker_id] = conn 
        worker_last_heartbeat[worker_id] = time.time() 
        worker_tasks[worker_id] = [] 
        print(f"[MASTER] Worker connected: {worker_id} from {addr}") 
 
        while True: 
            try: 
                data = conn.recv(4096) 
                if not data: 
                    break 
 
                msg = pickle.loads(data) 
 
                if msg.get("heartbeat"): 
                    worker_last_heartbeat[worker_id] = time.time() 
                    conn.sendall(pickle.dumps({"heartbeat_ack": True})) 
                elif msg.get("result"): 
                    task_id = msg["task_id"] 
                    print(f"[MASTER] Task {task_id} completed by {worker_id}") 
                    worker_tasks[worker_id].remove(task_id) 
            except: 
                break 
47 
 
    finally: 
        handle_worker_failure(worker_id) 
 
def heartbeat_monitor(): 
    while True: 
        time.sleep(2) 
        for worker_id in list(worker_last_heartbeat.keys()): 
            if time.time() - worker_last_heartbeat[worker_id] > HEARTBEAT_TIMEOUT: 
                handle_worker_failure(worker_id) 
 
def handle_worker_failure(worker_id): 
    if worker_id in workers: 
        print(f"\n[MASTER] Worker {worker_id} FAILED - no heartbeat") 
        failed_tasks = worker_tasks.get(worker_id, []) 
 
        print(f"[MASTER] Recovering {len(failed_tasks)} tasks from {worker_id}") 
 
        for task_id in failed_tasks: 
            print(f"[MASTER] Re-queuing Task {task_id} for reassignment") 
            tasks_queue.put({"id": task_id, "duration": 3}) 
 
        cleanup_worker(worker_id) 
        print("[MASTER] Task recovery complete\n") 
 
def cleanup_worker(worker_id): 
    if worker_id in workers: 
        try: 
            workers[worker_id].close() 
        except: 
            pass 
        del workers[worker_id] 
    worker_last_heartbeat.pop(worker_id, None) 
    worker_tasks.pop(worker_id, None) 
 
def assign_tasks(): 
    task_id = 1 
    while True: 
        time.sleep(2) 
        if not workers: 
            print("[MASTER]  No workers available to assign tasks") 
            continue 
        task = {"id": task_id, "duration": 5} 
                tasks_queue.put(task) 
48 
 
        print(f"[MASTER]  Task {task_id} queued") 
        task_id += 1 
 
        for worker_id, conn in list(workers.items()): 
            if not tasks_queue.empty(): 
                task = tasks_queue.get() 
                try: 
                    conn.sendall(pickle.dumps({"task": task})) 
                    worker_tasks[worker_id].append(task["id"]) 
                    print(f"[MASTER] Task {task['id']} assigned to {worker_id}") 
                except: 
                    handle_worker_failure(worker_id) 
 
def start_server(): 
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    server.bind((MASTER_HOST, MASTER_PORT)) 
    server.listen() 
    print(f"[MASTER] Listening on {MASTER_HOST}:{MASTER_PORT}") 
 
    threading.Thread(target=heartbeat_monitor, daemon=True).start() 
    threading.Thread(target=assign_tasks, daemon=True).start() 
 
    while True: 
        conn, addr = server.accept() 
        threading.Thread(target=client_handler, args=(conn, addr), daemon=True).start() 
 
if __name__ == "__main__": 
    start_server() 
