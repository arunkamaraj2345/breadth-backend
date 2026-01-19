from flask import Flask, jsonify, request
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time
import random
import traceback
import os

app = Flask(__name__)

# --------------------------------------------------
# CONFIG
# --------------------------------------------------

HOLIDAY_FILE = "holidays.csv"

# --------------------------------------------------
# HELPERS
# --------------------------------------------------

def normalize_symbol(symbol):
    symbol = str(symbol).strip().upper()
    if symbol.endswith(".NS") or symbol.endswith(".BO"):
        return symbol
    return symbol + ".NS"

def load_holidays():
    if not os.path.exists(HOLIDAY_FILE):
        return set()
    df = pd.read_csv(HOLIDAY_FILE, header=None)
    dates = pd.to_datetime(df.iloc[:, 0], errors="coerce")
    return set(dates.dropna().dt.date)

HOLIDAYS = load_holidays()

def sum_last(series, n):
    if len(series) < n:
        return "NIL"
    return float(series.iloc[-n:].sum())

def fetch_history(symbol, start, end):
    try:
        print(f"[FETCH] {symbol}")

        ticker = yf.Ticker(symbol)
        df = ticker.history(start=start, end=end, auto_adjust=False).reset_index()

        # micro sleep (human-like)
        time.sleep(0.2 + random.random() * 0.3)

        if df.empty:
            return None

        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date", "Close"])

        # apply holiday exclusion
        df = df[~df["Date"].dt.date.isin(HOLIDAYS)]

        return df

    except Exception as e:
        print(f"[ERROR] fetch_history failed for {symbol}: {e}")
        traceback.print_exc()
        return None

# --------------------------------------------------
# STATUS
# --------------------------------------------------

@app.route("/status", methods=["GET"])
def status():
    return jsonify({
        "status": "server on",
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

# --------------------------------------------------
# SINGLE STOCK ENGINE
# --------------------------------------------------

@app.route("/stock-engine", methods=["GET"])
def stock_engine():
    try:
        symbol = request.args.get("symbol")
        if not symbol:
            return jsonify({"error": "symbol parameter missing"}), 400

        symbol = normalize_symbol(symbol)

        today = datetime.today().date()
        start_date = today - timedelta(days=370)
        end_date = today + timedelta(days=1)

        df = fetch_history(symbol, start_date.isoformat(), end_date.isoformat())
        if df is None or df.empty:
            return jsonify({"error": "No data found"}), 404

        # drop possibly incomplete candle
        df = df.iloc[:-1]

        closes = df["Close"].astype(float)

        hard_data = {
            "symbol": symbol,
            "sum_19": sum_last(closes, 19),
            "sum_49": sum_last(closes, 49),
            "sum_99": sum_last(closes, 99),
            "sum_199": sum_last(closes, 199),
        }

        # 52W high
        try:
            hard_data["52w_high"] = float(df["High"].rolling(252).max().iloc[-1])
        except:
            hard_data["52w_high"] = "NIL"

        # ------------------------------------------
        # SOFT DATA (LATEST)
        # ------------------------------------------

        last = df.iloc[-1]

        soft_data = {
            "last_trading_date": str(last["Date"].date()),
            "close": float(last["Close"]),
            "high": float(last["High"])
        }

        return jsonify({
            "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "hard_data": hard_data,
            "soft_data": soft_data
        })

    except Exception as e:
        print("[FATAL] /stock-engine crashed:", e)
        traceback.print_exc()
        return jsonify({
            "error": "stock-engine failed",
            "details": str(e)
        }), 500

# --------------------------------------------------
# RUN SERVER
# --------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
