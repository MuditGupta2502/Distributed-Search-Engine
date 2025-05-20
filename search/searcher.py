# search/searcher.py
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config
import json
import math
import time
from collections import defaultdict
import glob
from pymongo import MongoClient
from utils.text_cleaner import extract_text_from_html, extract_snippet, highlight_terms, tokenize

# Connect to MongoDB
client = MongoClient(config.MONGO_CONN_STR)
db = client["distributed_crawler"]

INDEX_DIR = "data/index"
# Metadata is now loaded from MongoDB rather than a file.
def load_metadata():
    mapping = {}
    for record in db["metadata"].find({}):
        mapping[record["filename"]] = {
            "title": record["title"],
            "url": record["url"]
        }
    return mapping

class SearchEngine:
    def __init__(self):
        self.index = self.load_index()  # load index shards from disk
        self.metadata = load_metadata()
        self.total_docs = len(self.metadata)
        self.doc_freq = self.compute_doc_freq()
        self.doc_length = self.compute_doc_lengths()
        self.avg_dl = sum(self.doc_length.values()) / self.total_docs if self.total_docs > 0 else 0
        self.k1 = 1.5
        self.b = 0.75
        self.cache = {}
        self.cache_ttl = 300

    def load_index(self):
        index = {}
        shard_pattern = os.path.join(INDEX_DIR, "shard_*.json")
        shard_files = glob.glob(shard_pattern)
        if not shard_files:
            raise Exception(f"No shard files found matching {shard_pattern}.")
        for shard_file in shard_files:
            with open(shard_file, "r", encoding="utf-8") as f:
                shard_index = json.load(f)
                index.update(shard_index)
        return index

    def compute_doc_freq(self):
        df = {}
        for word, postings in self.index.items():
            df[word] = len(postings)
        return df

    def compute_doc_lengths(self):
        doc_length = {}
        for doc_id in self.metadata:
            page = db["pages"].find_one({"filename": doc_id})
            if page and "content" in page:
                html = page["content"]
                text = extract_text_from_html(html)
                tokens = tokenize(text)
                doc_length[doc_id] = len(tokens)
            else:
                doc_length[doc_id] = 0
        return doc_length

    def bm25_score(self, term, doc_id, freq):
        df = self.doc_freq.get(term, 1)
        idf = math.log((self.total_docs - df + 0.5) / (df + 0.5) + 1)
        dl = self.doc_length.get(doc_id, 0)
        numerator = freq * (self.k1 + 1)
        denominator = freq + self.k1 * (1 - self.b + self.b * (dl / self.avg_dl))
        return idf * (numerator / denominator)

    def search(self, query, top_k=10):
        current_time = time.time()
        if query in self.cache:
            cached_time, cached_results = self.cache[query]
            if current_time - cached_time < self.cache_ttl:
                print(f"Returning cached result for query: '{query}'")
                return cached_results
        words = query.lower().split()
        scores = defaultdict(float)
        for word in words:
            if word in self.index:
                for doc_id, freq in self.index[word]:
                    scores[doc_id] += self.bm25_score(word, doc_id, freq)
        results = []
        for doc_id in sorted(scores, key=scores.get, reverse=True)[:top_k]:
            meta = self.metadata.get(doc_id)
            if not meta:
                continue
            page = db["pages"].find_one({"filename": doc_id})
            snippet = ""
            if page and "content" in page:
                html = page["content"]
                text = extract_text_from_html(html)
                snippet = extract_snippet(text, words)
                snippet = highlight_terms(snippet, words)
            results.append({
                "title": meta["title"],
                "url": meta["url"],
                "score": round(scores[doc_id], 2),
                "snippet": snippet
            })
        self.cache[query] = (current_time, results)
        return results

if __name__ == "__main__":
    se = SearchEngine()
    query = input("Enter query: ")
    results = se.search(query)
    for res in results:
        print("-" * 40)
        print(f"Title: {res['title']}")
        print(f"URL  : {res['url']}")
        print(f"Score: {res['score']}")
        print(f"Snippet: {res['snippet']}")
