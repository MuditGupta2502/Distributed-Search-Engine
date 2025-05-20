# crawler/fetcher.py
import requests

def fetch(url, timeout=5):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"[Fetch Error] {url}: {e}")
        return None
