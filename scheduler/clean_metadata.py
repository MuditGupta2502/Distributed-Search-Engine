# scheduler/clean_metadata.py
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config
from pymongo import MongoClient

client = MongoClient(config.MONGO_CONN_STR)
db = client["distributed_crawler"]
metadata_collection = db["metadata"]
pages_collection = db["pages"]

def clean_metadata():
    kept = 0
    removed = 0
    for record in metadata_collection.find({}):
        filename = record.get("filename")
        if not filename:
            continue
        page = pages_collection.find_one({"filename": filename})
        if not page:
            print(f"Removing metadata for missing page: {filename}")
            metadata_collection.delete_one({"_id": record["_id"]})
            removed += 1
        else:
            kept += 1
    print(f"Cleaning complete. Kept {kept} records; removed {removed} records.")

if __name__ == "__main__":
    clean_metadata()
