import csv
import math
import random
import threading
import time
from collections import defaultdict
from datetime import datetime

from locust import HttpUser, LoadTestShape, between, events, task

RANDOM_SEED = 42
random.seed(RANDOM_SEED)

print(f"Using random seed: {RANDOM_SEED} (patterns will be reproducible)")


class RequestTracker:
    """
    Tracker for recording and logging HTTP requests to CSV.
    
    Records request counts per minute and logs them to a timestamped CSV file.
    Designed for monitoring load test request patterns over time.
    """
    
    def __init__(self):
        self.request_counts = defaultdict(int)
        self.start_time = None
        self.lock = threading.Lock()
        self.csv_filename = f"requests_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self.last_logged_minute = -1
        
    def record_request(self):
        if self.start_time is None:
            self.start_time = time.time()
            
        current_time = time.time()
        minute_mark = int((current_time - self.start_time) // 60)
        
        with self.lock:
            self.request_counts[minute_mark] += 1
    
    def initialize_csv(self):
        with open(self.csv_filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Timestamp', 'Requests_Per_Minute'])
    
    def log_minute_data(self):
        if self.start_time is None:
            return
            
        current_time = time.time()
        current_minute = int((current_time - self.start_time) // 60)
        
        with self.lock:
            for minute_to_log in range(self.last_logged_minute + 1, current_minute):
                requests_this_minute = self.request_counts[minute_to_log]
                minute_timestamp = datetime.fromtimestamp(self.start_time + minute_to_log * 60)
                timestamp_str = minute_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                
                with open(self.csv_filename, 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow([timestamp_str, requests_this_minute])
                
                self.last_logged_minute = minute_to_log
                print(f"Logged minute {minute_to_log}: {requests_this_minute} requests")


tracker = RequestTracker()


@events.request.add_listener
def on_request(request_type, name, response_time, response_length, response, 
               context, exception, **kwargs):
    tracker.record_request()


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    tracker.initialize_csv()


def csv_logger():
    while True:
        time.sleep(60)
        tracker.log_minute_data()


csv_thread = threading.Thread(target=csv_logger, daemon=True)
csv_thread.start()


class WebsiteUser(HttpUser):
    wait_time = between(0.5, 2)

    # host = 'http://192.168.49.2:30500/'
    host = ''
    
    @task
    def test_endpoint(self):
        """Make GET request to root endpoint."""
        response = self.client.get('/')


class MyLoadShape(LoadTestShape):
    """
    Creates a sinusoidal wave pattern of users with added random variation
    to simulate realistic traffic patterns. User count oscillates between
    min_users and max_users over the specified wave_length period.
    """
    
    def __init__(self):
        super().__init__()
        self.min_users = 25
        self.max_users = 150
        self.wave_length = 180
        self.random_factor = 0.2
        
    def tick(self):
        run_time = self.get_run_time()
        
        base_wave = math.sin(2 * math.pi * run_time / self.wave_length)
        random_noise = random.uniform(-self.random_factor, self.random_factor)
        wave_with_noise = base_wave + random_noise
        
        normalized = (wave_with_noise + 1) / 2
        normalized = max(0, min(1, normalized))
        
        user_count = int(self.min_users + (self.max_users - self.min_users) * normalized)
        spawn_rate = max(1, user_count // 10)
        
        return (user_count, spawn_rate)