from yadtq import YADTQWorker
import time
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the log level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define log format
    handlers=[
        logging.FileHandler("worker.log"),  # Write logs to 'worker.log'
        
    ]
)

def add(a, b):
    
    time.sleep(10)  # Simulate a long-running task
    result = a + b
    logging.info(f"'add' task completed: {a} + {b} = {result}")
    return result
    

def sub(a, b):
    
    time.sleep(10)  # Simulate a long-running task
    result = a - b
    logging.info(f"'sub' task completed: {a} - {b} = {result}")
    return result
    

def multiply(a, b):
    
    time.sleep(10)  # Simulate a long-running task
    result = a * b
    logging.info(f"'multiply' task completed: {a} * {b} = {result}")
    return result
    
def divide(a,b):
    
    time.sleep(10)  # Simulate a long-running task
    result = a / b
    logging.info(f"'divide' task completed: {a} / {b} = {result}")
    return result
    
def main():
    try:
        worker = YADTQWorker(group_id='worker-group1')
        task_functions = {
            'add': add,
            'sub': sub,
            'multiply': multiply,
            'divide' : divide
        }

        logging.info(f"Worker {worker.worker_id} started and ready to consume tasks.")
        print(f"Worker {worker.worker_id} started and ready to consume tasks.")
        # Error handling for task consumption
        try:
            worker.consume_tasks(task_functions)
        except Exception as e:
            logging.critical(f"Failed to consume tasks: {e}")
            raise

    except Exception as e:
        logging.critical(f"Worker initialization failed: {e}")

if __name__ == '__main__':
    main()
