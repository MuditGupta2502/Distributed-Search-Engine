import time
import os
import subprocess  # import subprocess to run external scripts
from concurrent.futures import ThreadPoolExecutor, as_completed
from crawler.distributed_frontier import DistributedFrontier
from crawler.fetcher import fetch
from crawler.parser import extract_links_and_title
from utils.storage import save_page, save_metadata

MAX_PAGES = 50
MAX_THREADS = 5

def load_seed_urls(primary="data/seed_urls.txt", backup="data/unique_urls.txt"):
    """
    Load seed URLs from primary file (seed_urls.txt).
    If the primary file is missing or empty, load from backup file (unique_urls.txt).
    If the backup file doesn't exist, run deduplication_urls.py to generate it.
    Returns a list of URLs.
    """
    seeds = []
    if os.path.exists(primary):
        with open(primary, "r", encoding="utf-8") as f:
            seeds = [line.strip() for line in f if line.strip()]
        if seeds:
            print("Loaded seeds from primary file:", seeds)
            return seeds
        else:
            print(f"Primary file '{primary}' is empty. Falling back to backup file.")
    else:
        print(f"Primary file '{primary}' not found. Falling back to backup file.")

    # If backup doesn't exist, run deduplication_urls.py to generate it.
    if not os.path.exists(backup):
        print(f"Backup file '{backup}' not found. Running deduplication script...")
        subprocess.run(["python3", "indexer/deduplication_urls.py"], check=True, cwd=os.getcwd())
    
    if os.path.exists(backup):
        with open(backup, "r", encoding="utf-8") as f:
            seeds = [line.strip() for line in f if line.strip()]
        if seeds:
            print("Loaded seeds from backup file:", seeds)
        else:
            print(f"Backup file '{backup}' is empty after deduplication.")
    else:
        print(f"Backup file '{backup}' still not found after running deduplication.")

    return seeds

def crawl_task(frontier: DistributedFrontier):
    while frontier.visited_count() < MAX_PAGES:
        url = frontier.get_next_url()
        if not url:
            print("No URL fetched; waiting...")
            continue
        print(f"[Distributed Worker] Crawling: {url}")
        html = fetch(url)
        if html:
            links, title = extract_links_and_title(html)
            filename = save_page(url, html)
            save_metadata(url, title, filename)
            print(f"Saved page {filename} for {url}")
            frontier.add_urls(links)
        else:
            print(f"Failed to fetch {url}")
        frontier.mark_visited(url)
        time.sleep(1)
        
def main():
    seeds = load_seed_urls()
    frontier = DistributedFrontier(seed_urls=seeds)
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(crawl_task, frontier) for _ in range(MAX_THREADS)]
        for future in as_completed(futures):
            future.result()
    print("Visited count:", frontier.visited_count())

if __name__ == "__main__":
    main()