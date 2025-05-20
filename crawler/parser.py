# crawler/parser.py
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def is_valid_link(href):
    """
    Returns True for links that look like navigable URLs.
    Excludes empty links, anchors, javascript, and mailto links.
    """
    if not href:
        return False
    href = href.strip()
    if href.startswith("#"):
        return False
    if href.lower().startswith("javascript:"):
        return False
    if href.lower().startswith("mailto:"):
        return False
    return True

def extract_links_and_title(html, base_url=""):
    """
    Extracts all valid links and the page title from the HTML.
    If base_url is provided, relative URLs will be converted to absolute.
    """
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.string.strip() if soup.title and soup.title.string else "Untitled"
    links = set()
    for a_tag in soup.find_all("a", href=True):
        href = a_tag['href']
        if is_valid_link(href):
            full_url = urljoin(base_url, href) if base_url else href
            links.add(full_url)
    return links, title
