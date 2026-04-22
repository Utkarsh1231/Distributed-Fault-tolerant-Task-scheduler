import time 
import random 
 
def task_sleep(task_id, duration=3): 
    print(f"[Task {task_id}] Sleeping for {duration} seconds") 
    time.sleep(duration) 
    return f"Task {task_id} finished sleeping." 
 
def task_random_fail(task_id): 
    print(f"[Task {task_id}] Starting random fail simulation") 
    if random.random() < 0.4:  # 40% chance to fail 
        raise Exception(f"Task {task_id} FAILED due to random error!") 
    time.sleep(2) 
    return f"Task {task_id} completed successfully."
