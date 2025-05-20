# search/query_coordinator.py
import requests

SEARCHER_NODES = [
    "http://localhost:8000/api/search",
    "http://localhost:8001/api/search"
]

def distributed_search(query, top_k=10):
    aggregated_results = []
    for node_url in SEARCHER_NODES:
        try:
            response = requests.get(node_url, params={"q": query, "top_k": top_k}, timeout=5)
            if response.status_code == 200:
                data = response.json()
                aggregated_results.extend(data.get("results", []))
            else:
                print(f"Non-200 response from {node_url}: {response.status_code}")
        except Exception as e:
            print(f"Error querying node {node_url}: {e}")
    aggregated_results.sort(key=lambda item: item.get("score", 0), reverse=True)
    return aggregated_results[:top_k]

if __name__ == "__main__":
    query = input("Enter your search query: ").strip()
    if query:
        results = distributed_search(query)
        for res in results:
            print("-" * 40)
            print(f"Title: {res.get('title')}")
            print(f"URL: {res.get('url')}")
            print(f"Score: {res.get('score')}")
            print(f"Snippet: {res.get('snippet')}")
    else:
        print("No query provided.")
