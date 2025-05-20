#!/usr/bin/env python3
import os
import sys
import logging
from datetime import datetime, timedelta, timezone

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config
from pymongo import MongoClient

# Configure logging
LOG_FILE = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')), "scheduler", "recrawl_scheduler.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

client = MongoClient(config.MONGO_CONN_STR)
db = client["distributed_crawler"]
metadata_collection = db["metadata"]

# Set threshold for recrawl, e.g., 0.01 days (~14.4 minutes) for testing
THRESHOLD_DAYS = 0.01

def recrawl_urls():
    recrawl_list = []
    now = datetime.now(timezone.utc)
    for record in metadata_collection.find({}):
        last_crawled = record.get("last_crawled")
        if last_crawled:
            try:
                last_time = datetime.fromisoformat(last_crawled.replace("Z", "+00:00"))
                if last_time.tzinfo is None:
                    last_time = last_time.replace(tzinfo=timezone.utc)
                if now - last_time > timedelta(days=THRESHOLD_DAYS):
                    recrawl_list.append(record["url"])
            except Exception as e:
                logging.error(f"Error parsing {last_crawled}: {e}")
                continue
    return recrawl_list

def main():
    logging.info("Recrawl scheduler started.")
    urls = recrawl_urls()
    if urls:
        logging.info(f"{len(urls)} URLs due for recrawl found.")
        try:
            from crawler.recrawl import crawl_urls  # Updated import from the new module
            crawl_urls(urls)  # Requeue these URLs for recrawl
            logging.info("Recrawling initiated for due URLs.")
        except Exception as e:
            logging.error(f"Error initiating recrawl: {e}")
    else:
        logging.info("No URLs due for recrawl at this time.")
    logging.info("Recrawl scheduler finished.")

if __name__ == "__main__":
    main()
