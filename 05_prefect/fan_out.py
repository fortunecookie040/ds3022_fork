import random
import time
from prefect import flow, task, get_run_logger

# @task(log_prints = True, retries = 3, retry_delay_seconds = 2)

@task
def print_and_sleep(value: int):
    delay = random.randint(5,15)
    time.sleep(delay)
    print(f"Value {value} (delayed {delay}s)")
    time.sleep(5)
    return value

@task
def range_task(start: int = 1, end: int = 20):
    futures = print_and_sleep.map(range(start, end + 1))
    results = [future.result() for future in futures]
    print(f"Completed{len(results)} subtasks")    
    return results

@flow
def fan_out_flow():
    range_task()

if __name__ == "__main__":
    fan_out_flow()
    # WITHOUT PARAMETERS, RUN BY DEFAULT
    # SHOULD BE AT THE BOTTOM OF THE SCRIPT
    # THIS SCRIPT BASICALLY CALLS ALL THE OTHER SCRIPTS BC ITS A CASCADE


