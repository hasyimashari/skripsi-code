import os
import sys
import signal
import threading

import csv
import math
import random
import time

from collections import defaultdict
from datetime import datetime

from locust import HttpUser, LoadTestShape, between, events, task
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('locust.log')
    ]
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

SEED = int(os.getenv('SEED'))
MIN_RPS = int(os.getenv('MIN_RPS'))    
MAX_RPS = int(os.getenv('MAX_RPS'))   
WAIT_TIME_MIN = float(os.getenv('WAIT_TIME_MIN'))    
WAIT_TIME_MAX = float(os.getenv('WAIT_TIME_MAX'))   
TREND_DURATION = int(os.getenv('TREND_DURATION'))   
TICK_INTERVAL = int(os.getenv('TICK_INTERVAL'))     
NOISE_FACTOR = float(os.getenv('NOISE_FACTOR'))  

AVG_WAIT = (WAIT_TIME_MIN + WAIT_TIME_MAX) / 2

def rps_to_users(rps):
    return max(1, int(rps * AVG_WAIT))

MIN_USERS = rps_to_users(MIN_RPS)
MAX_USERS = rps_to_users(MAX_RPS)



class RequestTracker:
    """
    Records how many requests were made each minute and flushes
    completed minutes to a timestamped CSV file.
    """
    def __init__(self):
        self.request_counts = defaultdict(int)
        self.start_time = None
        self.lock = threading.Lock()
        self.last_logged_minute = -1
        
        self.csv_dir = "./test-result"
        csv_filename = f"requests_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        self.csv_file = os.path.join(self.csv_dir, csv_filename)
        
    def record_request(self):
        if self.start_time is None:
            self.start_time = time.time()
            
        minute = int((time.time() - self.start_time) // 60)
        
        with self.lock:
            self.request_counts[minute] += 1

    def initialize_csv(self):
        os.makedirs(self.csv_dir, exist_ok=True)
        
        with open(self.csv_file, 'w', newline='') as f:
            csv.writer(f).writerow(['Timestamp', 'Minute', 'Requests_Per_Minute', 'Avg_RPS'])
            
        print(f"[tracker] CSV initialised → {self.csv_file}")

    def flush_completed_minutes(self):
        os.makedirs(self.csv_dir, exist_ok=True)
        
        if self.start_time is None:
            return
            
        current_minute = int((time.time() - self.start_time) // 60)
        
        with self.lock:
            for m in range(self.last_logged_minute + 1, current_minute):
                count    = self.request_counts[m]
                avg_rps  = round(count / 60, 2)
                ts       = datetime.fromtimestamp(self.start_time + m * 60) \
                                   .strftime('%Y-%m-%d %H:%M:%S')
                               
                with open(self.csv_file, 'a', newline='') as f:
                    csv.writer(f).writerow([ts, m, count, avg_rps])
                
                self.last_logged_minute = m
                
                print(f"[tracker] minute {m:>3} ({ts}) -> {count} requests  ({avg_rps} avg RPS)")

tracker = RequestTracker()

def _shutdown_handler(sig, frame):
    print("\n[shutdown] signal received, flushing remaining data")
    tracker.flush_completed_minutes()
    print(f"[shutdown] CSV saved -> {tracker.csv_file}")
    sys.exit(0)

signal.signal(signal.SIGINT, _shutdown_handler)
signal.signal(signal.SIGTERM, _shutdown_handler)

def _csv_logger():
    while True:
        time.sleep(60)
        tracker.flush_completed_minutes()

threading.Thread(target=_csv_logger, daemon=True).start()



class WebsiteUser(HttpUser):
    host      = os.getenv('TEST_APP_URL')
    wait_time = between(WAIT_TIME_MIN, WAIT_TIME_MAX)

    @task
    def test_endpoint(self):
        self.client.get('/')

class RandomLoad(LoadTestShape):
    """
    Sinusoidal wave where each half-cycle (up or down) lasts TREND_DURATION
    seconds. Floor and peak are defined in RPS and converted to user counts.

    One full cycle = 2 × TREND_DURATION (e.g. 10 min up + 10 min down)
    Pattern loops indefinitely.
    """
    def __init__(self):
        super().__init__()
        self.min_users     = MIN_USERS
        self.max_users     = MAX_USERS
        self.cycle_seconds = TREND_DURATION * 2
        self.tick_interval = TICK_INTERVAL
        self.noise_factor  = NOISE_FACTOR

        self._pattern = self._build_cycle(seed=SEED)
        self._log_pattern_summary()

    def _build_cycle(self, seed: int):
        rng        = random.Random(seed)
        user_range = self.max_users - self.min_users
        steps      = self.cycle_seconds // self.tick_interval
        pattern    = []

        for step in range(steps):
            t          = step * self.tick_interval
            sine_value = math.sin(math.pi * t / self.cycle_seconds)
            noise      = rng.uniform(-self.noise_factor, self.noise_factor)
            level      = max(0.0, min(1.0, sine_value + noise))
            user_count = int(self.min_users + user_range * level)
            spawn_rate = max(1, user_count // 10)
            pattern.append((user_count, spawn_rate))

        return pattern

    def tick(self):
        run_time   = self.get_run_time()
        step_index = int((run_time % self.cycle_seconds) // self.tick_interval)
        return self._pattern[step_index]

    def _log_pattern_summary(self):
        counts   = [u for u, _ in self._pattern]
        rps_vals = [round(u / AVG_WAIT, 1) for u in counts]
        print(
            f"[shape] cycle={self.cycle_seconds}s  "
            f"steps={len(self._pattern)}  "
            f"users min={min(counts)} max={max(counts)}  "
            f"est. RPS min={min(rps_vals)} max={max(rps_vals)}"
        )



@events.request.add_listener
def on_request(request_type, name, response_time, response_length,
               response, context, exception, **kwargs):
    tracker.record_request()

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    tracker.initialize_csv()
    print(f"[tracker] RPS target -> min={MIN_RPS}  max={MAX_RPS}")
    print(f"[tracker] User range -> min={MIN_USERS}  max={MAX_USERS}  (avg_wait={AVG_WAIT}s)")
    
@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    print("[tracker] test stopping -> flushing remaining data")
    tracker.flush_completed_minutes()
    print(f"[tracker] CSV saved -> {tracker.csv_file}")
