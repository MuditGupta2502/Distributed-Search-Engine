# crawler/recrawl.py
import config
from crawler.distributed_frontier import DistributedFrontier

def crawl_urls(url_list):
    """
    Requeue URLs for recrawling.
    This function creates a new DistributedFrontier instance (which connects to the shared Redis)
    and adds the URLs back to the crawl queue.
    """
    frontier = DistributedFrontier(seed_urls=None)
    # Add the recrawl URLs to the Redis queue.
    frontier.add_urls(url_list)
    print(f"Requeued {len(url_list)} URLs for recrawl.")
