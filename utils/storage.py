# utils/storage.py
import hashlib
from datetime import datetime
from pymongo import MongoClient

# Connect to MongoDB (adjust URI as needed)
client = MongoClient("mongodb://localhost:27017")
db = client["distributed_crawler"]

# Collections for pages and metadata.
pages_collection = db["pages"]
metadata_collection = db["metadata"]

def hash_url(url):
    return hashlib.md5(url.encode()).hexdigest()

def save_page(url, html):
    """Save the HTML content of the page in MongoDB and return a unique filename (hash)."""
    filename = f"{hash_url(url)}.html"
    doc = {
        "filename": filename,
        "url": url,
        "content": html,
        "last_crawled": datetime.utcnow().isoformat()
    }
    # Upsert to ensure uniqueness by filename.
    pages_collection.update_one({"filename": filename}, {"$set": doc}, upsert=True)
    return filename

def save_metadata(url, title, filename):
    """Save or update metadata for the page in MongoDB."""
    doc = {
        "url": url,
        "title": title,
        "filename": filename,
        "last_crawled": datetime.utcnow().isoformat()
    }
    metadata_collection.update_one({"filename": filename}, {"$set": doc}, upsert=True)
