# crawler/distributed_frontier.py
import redis
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config

class DistributedFrontier:
    def __init__(self,seed_urls=None):
        self.redis = redis.Redis(host=config.REDIS_HOST,port=config.REDIS_PORT,db=config.REDIS_DB,decode_responses=True)
        self.url_queue_key = "url_queue"
        self.visited_set_key = "visited_urls"
        if seed_urls:
            for url in seed_urls:
                if not self.redis.sismember(self.visited_set_key, url):
                    self.redis.rpush(self.url_queue_key, url)

    def get_next_url(self):
        result = self.redis.blpop(self.url_queue_key, timeout=5)
        if result:
            return result[1]
        return None

    def mark_visited(self, url):
        self.redis.sadd(self.visited_set_key, url)

    def add_urls(self, urls):
        for url in urls:
            if not self.redis.sismember(self.visited_set_key, url):
                self.redis.rpush(self.url_queue_key, url)

    def visited_count(self):
        return self.redis.scard(self.visited_set_key)
