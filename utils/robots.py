# utils/robots.py
from urllib.parse import urlparse
import urllib.robotparser

def get_crawl_delay(url, user_agent="*"):
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(robots_url)
    try:
        rp.read()
    except Exception as e:
        print(f"Error reading robots.txt for {url}: {e}")
        return None
    return rp.crawl_delay(user_agent)  # Returns None if not specified
