import redis
import time
import uuid
import multiprocessing
import logging
import os
import json


# Logging setup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),                        # console
        logging.FileHandler("redlock.log", mode="w"),  # log file
    ],
)
logger = logging.getLogger(__name__)


# Shared resource: a counter stored in a JSON file

COUNTER_FILE = "shared_counter.json"
COUNTER_LOCK = multiprocessing.Lock()   # local file-access guard (process-safe)

def _read_counter():
    if not os.path.exists(COUNTER_FILE):
        return 0
    with open(COUNTER_FILE, "r") as f:
        return json.load(f)["counter"]

def _write_counter(value):
    with open(COUNTER_FILE, "w") as f:
        json.dump({"counter": value}, f)

def increment_counter(client_id):
    """Read, increment, and persist the shared counter (called inside critical section)."""
    with COUNTER_LOCK:
        current = _read_counter()
        new_value = current + 1
        _write_counter(new_value)
    logger.info(f"Client-{client_id}: Counter incremented {current} → {new_value}")
    return new_value


# Redlock

RELEASE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

# Staggered start delays so processes don't all compete at t=0
CLIENT_START_DELAYS = [0, 1, 1, 1, 4]


class Redlock:
    def __init__(self, redis_nodes):
        self.nodes = []
        for host, port in redis_nodes:
            try:
                client = redis.Redis(host=host, port=port, socket_connect_timeout=1)
                client.ping()
                self.nodes.append(client)
                logger.debug(f"Connected to Redis node {host}:{port}")
            except redis.RedisError:
                self.nodes.append(None)
                logger.warning(f"Redis node {host}:{port} is unreachable – treated as failed")

        self.quorum = len(redis_nodes) // 2 + 1
        reachable = sum(1 for n in self.nodes if n is not None)
        logger.info(f"Redlock initialised: {reachable}/{len(redis_nodes)} nodes reachable, quorum={self.quorum}")

    def _try_acquire_on_node(self, node, resource, lock_id, ttl):
        if node is None:
            return False
        try:
            return node.set(resource, lock_id, nx=True, px=ttl) is True
        except redis.RedisError as e:
            logger.debug(f"Node error during acquire: {e}")
            return False

    def acquire_lock(self, resource, ttl):
        lock_id = str(uuid.uuid4())
        start_time = time.time()

        acquired_count: int = sum(
            1 for node in self.nodes
            if self._try_acquire_on_node(node, resource, lock_id, ttl)
        )

        elapsed_ms = (time.time() - start_time) * 1000
        validity_time = ttl - elapsed_ms

        if acquired_count >= self.quorum and validity_time > 0:
            return True, lock_id
        else:
            self.release_lock(resource, lock_id)   # roll back any partial locks
            return False, None

    def release_lock(self, resource, lock_id):
        for node in self.nodes:
            if node is None:
                continue
            try:
                node.eval(RELEASE_SCRIPT, 1, resource, lock_id)
            except redis.RedisError:
                pass  


# Client process

def client_process(redis_nodes, resource, ttl, client_id):
    delay = CLIENT_START_DELAYS[client_id]
    if delay:
        time.sleep(delay)

    redlock = Redlock(redis_nodes)

    while True:
        logger.info(f"Client-{client_id}: Attempting to acquire lock…")
        lock_acquired, lock_id = redlock.acquire_lock(resource, ttl)

        if lock_acquired:
            logger.info(f"Client-{client_id}: Lock acquired! (id={lock_id})")

            # --- Critical section
            new_val = increment_counter(client_id)
            logger.info(f"Client-{client_id}: Finished critical section. Counter is now {new_val}.")
            time.sleep(2)   
           

            redlock.release_lock(resource, lock_id)
            logger.info(f"Client-{client_id}: Lock released.")
            break   
        else:
            logger.warning(f"Client-{client_id}: Failed to acquire lock. Retrying in 1 s…")
            time.sleep(1)


# Entry point

if __name__ == "__main__":
    redis_nodes = [
        ("localhost", 63791),
        ("localhost", 63792),
        ("localhost", 63793),
        ("localhost", 63794),
        ("localhost", 63795),
    ]

    resource = "shared_resource"
    ttl = 5000          # Lock TTL in milliseconds (5 s)
    num_clients = 5

    # Initialise the counter file to 0
    _write_counter(0)
    logger.info("Shared counter initialised to 0.")

    # Launch client processes
    processes = []
    for i in range(num_clients):
        p = multiprocessing.Process(
            target=client_process,
            args=(redis_nodes, resource, ttl, i),
        )
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    # Print the final counter value
    final = _read_counter()
    logger.info(f"All clients finished. Final counter value: {final}")
