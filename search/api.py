# search/api.py
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from search.searcher import SearchEngine

app = FastAPI()
engine = SearchEngine()
templates = Jinja2Templates(directory="search/templates")

@app.get("/search", response_class=HTMLResponse)
def search_html(request: Request, q: str = Query(default=None)):
    results = engine.search(q) if q else []
    return templates.TemplateResponse("search.html", {"request": request, "results": results, "query": q})

@app.get("/api/search")
def search_api(q: str = Query(...)):
    results = engine.search(q)
    return JSONResponse(content={"results": results})
