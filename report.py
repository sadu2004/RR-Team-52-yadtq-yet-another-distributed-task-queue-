import redis
from rich.console import Console
from rich.table import Table
import time
import os

console = Console()

# Connect to Redis
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

# def display_tasks():
#     table = Table(title="Task Monitoring")

#     table.add_column("Task ID", justify="right", style="cyan")
#     table.add_column("Status", justify="center", style="magenta")
#     table.add_column("Result", justify="center", style="green")
#     table.add_column("Assigned Worker", justify="center", style="yellow")

#     # Get all tasks
#     tasks_found = False
#     for key in redis_client.scan_iter(match="task:*"):
#         if redis_client.type(key) == 'hash':
#             tasks_found = True
#             task_info = redis_client.hgetall(key)
#             task_id = key.split(":")[1]
#             table.add_row(
#                 task_id,
#                 task_info.get('status', 'unknown'),
#                 task_info.get('result', 'N/A'),
#                 task_info.get('worker_id', 'N/A')
#             )
    
#     if tasks_found:
#         console.print(table)
#     else:
#         console.print("[yellow]No tasks found[/yellow]")

def display_workers():
    table = Table(title="Worker Monitoring")

    table.add_column("Worker ID", justify="right", style="cyan")
    table.add_column("Status", justify="center", style="magenta")

    # Fetch all workers from the 'worker_status' hash
    workers_found = False
    worker_statuses = redis_client.hgetall('worker_status')
    for worker_id, status in worker_statuses.items():
        workers_found = True
        last_heartbeat = redis_client.get(f"worker:{worker_id}:heartbeat")  # Fetch the last heartbeat timestamp
        table.add_row(
            worker_id,
            status
        )

    if workers_found:
        console.print(table)
    else:
        console.print("[yellow]No active workers found[/yellow]")

    if workers_found:
        console.print(table)
    else:
        console.print("[yellow]No active workers found[/yellow]")

if __name__ == '__main__':
    try:
        while True:
            os.system('clear')  # Clear screen more reliably
            print("\n=== YADTQ Monitoring ===\n")
            # display_tasks()
            # print()  # Add spacing between tables
            display_workers()
            print("\nRefreshing in 5 seconds... Press Ctrl+C to exit")
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nMonitoring stopped")
# workers=redis_client.hgetall('worker_status')
# for worker_id in workers:
#     redis_client.hset('worker_status',worker_id,'failed')
# print("all workers marked fail")
