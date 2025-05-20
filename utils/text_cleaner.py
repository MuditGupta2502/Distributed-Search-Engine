# utils/text_cleaner.py
import re
from bs4 import BeautifulSoup

def extract_text_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text(separator=" ", strip=True)

def tokenize(text):
    text = text.lower()
    return re.findall(r'\b[a-z]+\b', text)

def extract_snippet(text, query_words, max_sentences=2):
    sentences = re.split(r'(?<=[.!?])\s+', text)
    found = []
    for s in sentences:
        if any(q in s.lower() for q in query_words):
            found.append(s.strip())
        if len(found) >= max_sentences:
            break
    return " ".join(found)

def highlight_terms(text, query_words):
    for word in query_words:
        pattern = re.compile(re.escape(word), re.IGNORECASE)
        text = pattern.sub(r'<mark>\g<0></mark>', text)
    return text
