from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from data_loader import load_btc_history
from risk_engine import compute_risk_engine

app = FastAPI()
templates = Jinja2Templates(directory="templates")


def build_annotation(drivers: dict) -> str:
    if drivers["Volatility Expansion"] > 140:
        return "Volatility expanding rapidly"
    if drivers["Drawdown Stress"] > 80:
        return "Price near recent drawdown levels"
    if drivers["Tail Risk"] > 60:
        return "Increased frequency of extreme losses"
    return "Normal risk dynamics"


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    df = load_btc_history(90)

    risk_history = []

    for i in range(60, len(df)):
        slice_df = df.iloc[: i + 1]
        result = compute_risk_engine(slice_df)

        annotation = build_annotation(result["drivers"])
        main_driver = max(result["drivers"], key=result["drivers"].get)

        risk_history.append({
            "date": slice_df.iloc[-1]["timestamp"].date(),
            "risk_score": result["risk_score"],
            "regime": result["regime"],
            "drivers": result["drivers"],
            "main_driver": main_driver,
            "annotation": annotation
        })

    latest = risk_history[-1]

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "today": latest,
            "risk_history": risk_history
        }
    )

