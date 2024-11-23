# client.py

from yadtq import YADTQClient
import time

def main():
    client = YADTQClient()
    # Submit tasks
    task_ids = []
    task_ids.append(client.submit_task('divide', [1, 0]))

    task_ids.append(client.submit_task('add', [1, 2]))
    task_ids.append(client.submit_task('sub', [5, 3]))
    task_ids.append(client.submit_task('multiply', [2, 4]))
    task_ids.append(client.submit_task('add', [10, 20]))
    task_ids.append(client.submit_task('sub', [15, 7]))
    task_ids.append(client.submit_task('multiply', [3, 5]))

    # Periodically check task status
    pending = set(task_ids)
    while pending:
        for task_id in list(pending):
            status_info = client.get_task_status(task_id)
            status = status_info.get('status')
            if status in ['success', 'failed']:
                print(f'Task {task_id} completed with status: {status}, result: {status_info.get("result")}, error: {status_info.get("error")}')
                pending.remove(task_id)
            else:
                print(f'Task {task_id} status: {status}')
        time.sleep(2)  # Wait before checking again
    status_info1=client.get_task_status(task_ids[0])
    print(status_info1.get("result"))
if __name__ == '__main__':
    main()
