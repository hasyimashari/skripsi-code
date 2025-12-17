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
        """
        Initialize RequestTracker with default values.
        
        Creates a new CSV file with timestamp and sets up tracking structures
        for counting requests per minute.
        """
        self.request_counts = defaultdict(int)
        self.start_time = None
        self.lock = threading.Lock()
        self.csv_filename = f"requests_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self.last_logged_minute = -1
        
    def record_request(self):
        """
        Record a single request in the current minute bucket.
        
        Initializes start_time on first request and increments the counter
        for the current minute mark.
        """
        if self.start_time is None:
            self.start_time = time.time()
            
        current_time = time.time()
        minute_mark = int((current_time - self.start_time) // 60)
        
        with self.lock:
            self.request_counts[minute_mark] += 1
    
    def initialize_csv(self):
        """
        Create CSV file and write header row.
        
        Sets up the CSV file with columns for timestamp and requests per minute.
        """
        with open(self.csv_filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Timestamp', 'Requests_Per_Minute'])
    
    def log_minute_data(self):
        """
        Log all completed minutes that haven't been written to CSV yet.
        
        Writes request counts for each completed minute to the CSV file with
        corresponding timestamps. Prints confirmation for each logged minute.
        """
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
    """Record each request in the tracker."""
    tracker.record_request()


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Initialize CSV file when test starts."""
    tracker.initialize_csv()


def csv_logger():
    """
    Background thread function for periodic CSV logging.
    
    Runs continuously, logging completed minute data every 60 seconds.
    """
    while True:
        time.sleep(60)
        tracker.log_minute_data()


csv_thread = threading.Thread(target=csv_logger, daemon=True)
csv_thread.start()


class WebsiteUser(HttpUser):
    """
    Locust user class for load testing.
    
    Simulates user behavior by making HTTP GET requests to the root endpoint
    with random wait times between requests.
    """
    
    wait_time = between(0.5, 2)
    host = 'http://192.168.49.2:30500/'
    
    @task
    def test_endpoint(self):
        """Make GET request to root endpoint."""
        response = self.client.get('/')


class MyLoadShape(LoadTestShape):
    """
    Smooth wave pattern load shape with random noise.
    
    Creates a sinusoidal wave pattern of users with added random variation
    to simulate realistic traffic patterns. User count oscillates between
    min_users and max_users over the specified wave_length period.
    """
    
    def __init__(self):
        """
        Initialize load shape parameters.
        
        Sets up wave pattern with minimum 25 users, maximum 150 users,
        180-second wave cycle, and 20% random noise factor.
        """
        super().__init__()
        self.min_users = 25
        self.max_users = 150
        self.wave_length = 180
        self.random_factor = 0.2
        
    def tick(self):
        """
        Calculate user count and spawn rate for current time.
        
        Combines sinusoidal wave with random noise to determine the number
        of users. Spawn rate is calculated as 10% of target user count.
        
        Returns:
            tuple: (user_count, spawn_rate) for this tick
        """
        run_time = self.get_run_time()
        
        base_wave = math.sin(2 * math.pi * run_time / self.wave_length)
        random_noise = random.uniform(-self.random_factor, self.random_factor)
        wave_with_noise = base_wave + random_noise
        
        normalized = (wave_with_noise + 1) / 2
        normalized = max(0, min(1, normalized))
        
        user_count = int(self.min_users + (self.max_users - self.min_users) * normalized)
        spawn_rate = max(1, user_count // 10)
        
        return (user_count, spawn_rate)