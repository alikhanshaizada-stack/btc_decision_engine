from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime

from data_loader import load_or_fetch_data
from risk_engine import compute_risk

app = FastAPI()
templates = Jinja2Templates(directory="templates")

cached = None
last_updated = None


def update():
    global cached, last_updated
    df = load_or_fetch_data()
    cached = compute_risk(df)
    last_updated = datetime.utcnow()


update()


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            **cached,
            "last_updated": last_updated.strftime("%Y-%m-%d %H:%M UTC"),
        },
    )
