import redis
import threading
import time
import json

# Connect to Redis
redis_client = redis.StrictRedis(host='mytestelasticache.hw74hc.ng.0001.use1.cache.amazonaws.com', port=6379, db=0)

# Expiration time for cache in seconds
CACHE_EXPIRATION = 60

# Define a lock key for request coalescing
LOCK_KEY = "resource_lock"
RESOURCE_KEY = "resource_data"

# Simulate an expensive request that returns a complex result
def expensive_request():
    print("Fetching resource from external source...")
    time.sleep(5)  # Simulate a time-consuming request
    return {
        "data": "Expensive resource result",
        "timestamp": time.time(),
        "extra_info": {"field1": "value1", "field2": "value2", "field3": "value3"}
    }

# Function to handle request coalescing with Redis and return a subset
def get_resource_with_coalescing(subset_key=None):
    # Try to get the data from Redis cache
    cached_data = redis_client.get(RESOURCE_KEY)

    if cached_data:
        print("Returning cached data.")
        cached_data = json.loads(cached_data)  # Decode the cached data (JSON)
        
        # If a specific subset is requested
        if subset_key and subset_key in cached_data:
            return cached_data[subset_key]
        return cached_data

    # Use Redis lock to prevent duplicate requests
    lock_acquired = redis_client.setnx(LOCK_KEY, 1)

    if lock_acquired:
        # Set expiration time for the lock to avoid deadlock
        redis_client.expire(LOCK_KEY, 10)
        try:
            # Fetch the resource
            data = expensive_request()

            # Cache the result with expiration time (as JSON string)
            redis_client.set(RESOURCE_KEY, json.dumps(data), ex=CACHE_EXPIRATION)
            print("Data cached.")
        finally:
            # Release the lock
            redis_client.delete(LOCK_KEY)

        # Return full data or subset based on the request
        if subset_key and subset_key in data:
            return data[subset_key]
        return data
    else:
        # Wait until another process/thread finishes fetching the data
        print("Waiting for another thread to fetch the data...")
        while not cached_data:
            time.sleep(0.1)  # Sleep briefly before checking again
            cached_data = redis_client.get(RESOURCE_KEY)

        cached_data = json.loads(cached_data)  # Decode the cached data (JSON)
        print("Returning cached data after wait.")

        # Return full data or subset based on the request
        if subset_key and subset_key in cached_data:
            return cached_data[subset_key]
        return cached_data

# Example usage with threads to simulate multiple requests
def thread_request(subset_key=None):
    resource = get_resource_with_coalescing(subset_key=subset_key)
    print(f"Thread {threading.current_thread().name} got resource: {resource}")

# Create multiple threads simulating concurrent requests
threads = [
    threading.Thread(target=thread_request),  # Full data
    threading.Thread(target=thread_request, args=("timestamp",)),  # Subset
    threading.Thread(target=thread_request, args=("extra_info",)),  # Subset

    threading.Thread(target=thread_request, args=("extra_info",)),  # Subset
    threading.Thread(target=thread_request, args=("timestamp",))   # Subset
]

# Start all threads
for thread in threads:
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()
