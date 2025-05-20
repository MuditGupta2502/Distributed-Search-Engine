# mongodb_storage.py
import os
import json
import hashlib
from datetime import datetime
from pymongo import MongoClient

# Connection settings (adjust MONGO_URI as needed)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(MONGO_URI)
db = client.distributed_search  # Database name

# Collections for storing data
pages_collection = db.pages         # Stores HTML pages
metadata_collection = db.metadata     # Stores page metadata
index_collection = db.index           # Stores index shards

def hash_url(url):
    return hashlib.md5(url.encode()).hexdigest()

def save_page(url, html):
    """
    Save the HTML content of a page to MongoDB.
    Returns a filename (hash-based) identifier.
    """
    filename = f"{hash_url(url)}.html"
    document = {
        "filename": filename,
        "url": url,
        "html": html,
        "last_crawled": datetime.utcnow()
    }
    pages_collection.update_one({"filename": filename}, {"$set": document}, upsert=True)
    return filename

def get_page(filename):
    """Retrieve the HTML content for a page by its filename."""
    doc = pages_collection.find_one({"filename": filename})
    if doc:
        return doc.get("html", "")
    return None

def save_metadata(url, title, filename):
    """
    Save metadata for a page (including URL, title, filename, and crawl timestamp).
    """
    document = {
        "url": url,
        "title": title,
        "filename": filename,
        "last_crawled": datetime.utcnow()
    }
    metadata_collection.update_one({"filename": filename}, {"$set": document}, upsert=True)

def get_all_metadata():
    """Return a dictionary mapping filename to metadata for all pages."""
    mapping = {}
    for entry in metadata_collection.find():
        mapping[entry["filename"]] = {
            "title": entry["title"],
            "url": entry["url"],
            "last_crawled": entry["last_crawled"]
        }
    return mapping

def save_index_shard(shard_id, shard_index):
    """
    Save a sharded inverted index into MongoDB.
    Each shard is stored as a document with a shard_id key.
    """
    doc = {
        "shard_id": shard_id,
        "index": shard_index,
        "created_at": datetime.utcnow()
    }
    index_collection.update_one({"shard_id": shard_id}, {"$set": doc}, upsert=True)

def get_all_index_shards():
    """
    Retrieve all index shard documents from MongoDB and merge them into a single index dictionary.
    """
    index_data = {}
    for doc in index_collection.find():
        index_data.update(doc.get("index", {}))
    return index_data
