import json
import time
import random
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

SOURCES = ["direct", "search_engine", "facebook", "instagram", "twitter", "afiliate"]

# List of possible URLs/pages
PAGES = [
    "/home",
    "/about",
    "/contact",
    "/support",
    "/products",
    "/cart",
    "/order-success"
]

# Identify product types for order tracking
PRODUCT_TYPES = [
    "laptops", "smartphones", "accessories"
]

# List of possible user IDs (for tracking sessions)
USER_NAMES = [f"user_{i:04d}" for i in range(1, 21)]

# Define the delay between events in seconds
EVENT_DELAY = 1

# --- Event Generation Function ---
def generate_pageview_event():

    event_time = datetime.now(timezone.utc)
    user_name = random.choice(USER_NAMES)
    page_url = random.choice(PAGES)
    session_id = f"sess_{user_name}_{event_time.strftime('%H%M%S')}"

    referrer = random.choice(SOURCES) 
    geolocation = random.choice(["India", "USA", "UK", "Japan", "Germany", "Australia"])
    page_load_time_ms = random.randint(100, 3000)
    
    event = {
        "timestamp": event_time.isoformat(),
        "user_name": user_name,
        "session_id": session_id,
        "page_url": page_url,
        "event_type": "page_view",
        "product_category": "",
        "source": referrer,
        "geolocation": geolocation,
        "page_load_time_ms": page_load_time_ms
    }

    if page_url in ["/products", "/order-success"]:
        event["product_category"] = random.choice(PRODUCT_TYPES)
        if page_url == "/order-success":
            event["source"] = "direct"

    return event

# --- Main Simulation Loop ---
def start_stream():
    """The main loop that continuously generates and prints events."""
    
    TOPIC_NAME = 'raw_page_analytics'
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    try:
        while True:
            event = generate_pageview_event()
            event_json = json.dumps(event)
            
            print(event_json, flush=True)
            producer.send(TOPIC_NAME, value=event_json)
            
            time.sleep(EVENT_DELAY)
            
    except KeyboardInterrupt:
        print("\n--- Stream simulation stopped by user. ---")
    
    producer.close()

if __name__ == "__main__":
    start_stream()

