#!/usr/bin/env python
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config
import json
from pyspark.sql import SparkSession
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient(config.MONGO_CONN_STR)
db = client["distributed_crawler"]
metadata_collection = db["metadata"]

OUTPUT_PATH = "data/unique_urls.txt"

def main():
    spark = SparkSession.builder.appName("URLDeduplication").getOrCreate()
    
    # Load metadata from MongoDB using PyMongo
    metadata_docs = list(metadata_collection.find({}))
    
    # Create an RDD from the list of metadata documents.
    rdd = spark.sparkContext.parallelize(metadata_docs)
    
    urls = rdd.map(lambda doc: doc["url"])
    unique_urls = urls.distinct().collect()
    
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        for url in unique_urls:
            f.write(url + "\n")
    
    print(f"Deduplicated URLs saved to: {OUTPUT_PATH}")
    spark.stop()

if __name__ == "__main__":
    main()
