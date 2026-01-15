# =========================
# MINIMAL BREADTH BACKEND
# SUFFIX-ONLY FIX
# =========================

from flask import Flask, jsonify, request
import yfinance as yf
import pandas as pd
from datetime import datetime
import os

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UNIVERSE_DIR = os.path.join(BASE_DIR, "universes")
HOLIDAY_FILE = os.path.join(BASE_DIR, "holidays.csv")

# -------------------------
# SYMBOL NORMALIZATION
# -------------------------

def normalize_symbol(symbol):
    if not isinstance(symbol, str):
        return None

    symbol = symbol.strip().upper()

    if not symbol:
        return None

    if symbol.endswith(".NS") or symbol.endswith(".BO"):
        return symbol

    return symbol + ".NS"

# -------------------------
# LOAD HOLIDAYS
# -------------------------

def load_holidays():
    if not os.path.exists(HOLIDAY_FILE):
        return set()

    df = pd.read_csv(HOLIDAY_FILE, header=None)
    return {
        datetime.strptime(str(d).strip(), "%Y-%m-%d").date()
        for d in df.iloc[:, 0]
    }

HOLIDAYS = load_holidays()

# -------------------------
# LOAD UNIVERSES
# -------------------------

def load_universes():
    universes = {}

    for file in os.listdir(UNIVERSE_DIR):
        if file.endswith(".csv"):
            name = file.replace(".csv", "").upper()
            df = pd.read_csv(os.path.join(UNIVERSE_DIR, file), header=None)

            symbols = (
                df.iloc[:, 0]
                .apply(normalize_symbol)
                .dropna()
                .tolist()
            )

            universes[name] = symbols

    return universes

UNIVERSES = load_universes()

# -------------------------
# HELPERS
# -------------------------

def exclude_holidays(df):
    return df[~df.index.date.astype(object).isin(HOLIDAYS)]

def compute_sum(series, window):
    if len(series) < window:
        return None
    return float(series.iloc[-window:].sum())

# -------------------------
# DAILY (HEAVY)
# -------------------------

HARD_DATA = {}

@app.route("/run-daily", methods=["GET"])
def run_daily():
    universe = request.args.get("universe", "").upper()

    if universe not in UNIVERSES:
        return {"error": "Invalid universe"}, 400

    attempted = 0
    empty_hist = 0
    insufficient = 0
    loaded = 0

    HARD_DATA[universe] = {}

    for symbol in UNIVERSES[universe]:
        attempted += 1

        try:
            t = yf.Ticker(symbol)
            hist = t.history(period="400d", interval="1d")

            if hist.empty:
                empty_hist += 1
                continue

            hist = exclude_holidays(hist)

            if len(hist) < 20:
                insufficient += 1
                continue

            closes = hist.iloc[:-1]["Close"]

            sum_19 = compute_sum(closes, 19)
            sum_49 = compute_sum(closes, 49)
            sum_99 = compute_sum(closes, 99)
            sum_199 = compute_sum(closes, 199)

            info = t.info
            fifty_two_high = info.get("fiftyTwoWeekHigh")

            HARD_DATA[universe][symbol] = {
                "sum_19": sum_19,
                "sum_49": sum_49,
                "sum_99": sum_99,
                "sum_199": sum_199,
                "52w_high": fifty_two_high
            }

            loaded += 1

        except:
            empty_hist += 1

    return {
        "universe": universe,
        "attempted": attempted,
        "empty_history": empty_hist,
        "insufficient_history": insufficient,
        "stocks_loaded": loaded
    }

# -------------------------
# INTRADAY (LIGHT)
# -------------------------

@app.route("/breadth", methods=["GET"])
def breadth():
    universe = request.args.get("universe", "").upper()

    if universe not in HARD_DATA:
        return {"error": "Run /run-daily first"}, 400

    counts = {
        "20MA": {"above": 0, "available": 0},
        "50MA": {"above": 0, "available": 0},
        "100MA": {"above": 0, "available": 0},
        "200MA": {"above": 0, "available": 0},
    }

    new_highs = 0
    high_available = 0

    for symbol, hd in HARD_DATA[universe].items():
        try:
            hist = yf.Ticker(symbol).history(period="2d", interval="1d")
            if hist.empty:
                continue

            price = float(hist["Close"].iloc[-1])
            high = float(hist["High"].iloc[-1])

            ma_map = {
                "20MA":  (hd["sum_19"]  + price) / 20 if hd["sum_19"]  else None,
                "50MA":  (hd["sum_49"]  + price) / 50 if hd["sum_49"]  else None,
                "100MA": (hd["sum_99"]  + price) / 100 if hd["sum_99"] else None,
                "200MA": (hd["sum_199"] + price) / 200 if hd["sum_199"] else None,
            }

            for ma, value in ma_map.items():
                if value is None:
                    continue
                counts[ma]["available"] += 1
                if price / value >= 1:
                    counts[ma]["above"] += 1

            if high is not None and hd["52w_high"] is not None:
                high_available += 1
                if high >= hd["52w_high"]:
                    new_highs += 1

        except:
            continue

    result = {}

    for ma, d in counts.items():
        if d["available"] > 0:
            result[ma] = {
                "count": d["above"],
                "pct": d["above"] / d["available"]
            }

    return jsonify({
        "breadth": result,
        "new_52w_highs": {
            "count": new_highs,
            "pct": new_highs / high_available if high_available else 0
        }
    })

# -------------------------
# START SERVER
# -------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
