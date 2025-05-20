import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config
from pyspark.sql import SparkSession
import json
from utils.text_cleaner import extract_text_from_html, tokenize
from pymongo import MongoClient

# No global MongoDB connection is created here.
# Instead, we will initialize a connection per partition in our mapPartitions function.

def load_metadata_from_mongo():
    """
    Load all metadata documents from MongoDB using PyMongo (executed on the driver).
    This function runs on the driver, so it is safe to use a global connection here.
    """
    client = MongoClient(config.MONGO_CONN_STR)
    db = client["distributed_crawler"]
    metadata_collection = db["metadata"]
    # Return a list of metadata documents.
    metadata_docs = list(metadata_collection.find({}))
    client.close()
    return metadata_docs

def extract_doc_content_partition(iterator):
    """
    This function is applied on each partition. It creates a new MongoDB connection
    and processes each metadata document in the partition, returning (doc_id, text).
    """
    client = MongoClient("mongodb://localhost:27017")
    db = client["distributed_crawler"]
    pages_collection = db["pages"]
    
    for entry in iterator:
        doc_id = entry["filename"]
        page_doc = pages_collection.find_one({"filename": doc_id})
        if not page_doc:
            print(f"Error: No page found for {doc_id}")
            continue
        try:
            html = page_doc["content"]
            text = extract_text_from_html(html)
            yield (doc_id, text)
        except Exception as e:
            print(f"Error processing {doc_id}: {e}")
    client.close()

def main():
    spark = SparkSession.builder.appName("InvertedIndex").getOrCreate()
    
    # Load metadata from MongoDB on the driver.
    metadata_docs = load_metadata_from_mongo()
    
    # Create an RDD from the metadata documents.
    metadata_rdd = spark.sparkContext.parallelize(metadata_docs)
    
    # Use mapPartitions to extract document content with a fresh MongoDB connection per partition.
    docs_rdd = metadata_rdd.mapPartitions(extract_doc_content_partition).filter(lambda x: x is not None)
    
    # Build word-document pairs:
    # For each document, tokenize the text and emit ((word, doc_id), 1)
    word_doc_pairs = (
        docs_rdd.flatMap(lambda x: [((word, x[0]), 1) for word in tokenize(x[1])])
                .reduceByKey(lambda a, b: a + b)   # Sum counts per word/document pair
                .map(lambda x: (x[0][0], (x[0][1], x[1])))  # (word, (doc_id, frequency))
                .groupByKey()
                .mapValues(list)
    )
    
    # Custom partitioning (sharding): assign each word to a shard based on hash(word) mod NUMBER_OF_SHARDS.
    NUMBER_OF_SHARDS = 4
    sharded_rdd = word_doc_pairs.map(
        lambda kv: (hash(kv[0]) % NUMBER_OF_SHARDS, (kv[0], kv[1]))
    ).groupByKey().mapValues(list)
    
    # Collect all shard data as a dictionary.
    sharded_index = sharded_rdd.collectAsMap()
    
    # Create the output directory for index shards.
    INDEX_DIR = "data/index"
    os.makedirs(INDEX_DIR, exist_ok=True)
    
    # Write each shard to a separate JSON file.
    for shard_id, items in sharded_index.items():
        shard_index = { word: postings for word, postings in items }
        output_file = os.path.join(INDEX_DIR, f"shard_{shard_id}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(shard_index, f, indent=2)
        print(f"✅ Shard {shard_id} saved to: {output_file}")
    
    spark.stop()

if __name__ == "__main__":
    main()
