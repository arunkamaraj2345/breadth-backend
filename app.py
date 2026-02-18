from flask import Flask, jsonify, request 
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time
import random
import traceback
import os
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# --------------------------------------------------
# STATUS + FULL WARMUP (PLACED AT TOP)
# --------------------------------------------------

@app.route("/status", methods=["GET"])
def status():
    try:
        print("=== FULL WARMUP START ===")

        ticker = yf.Ticker("RELIANCE.NS")

        ticker.history(period="5d")
        time.sleep(2)

        # keep warm but NOT required elsewhere
        _ = ticker.fast_info

        time.sleep(1)

        pd.to_datetime(["2024-01-01","2024-01-02"])
        pd.DataFrame({"a":[1,2,3]}).astype(float)

        time.sleep(2)

        print("=== FULL WARMUP COMPLETE ===")

        return jsonify({
            "status": "server on",
            "warmup status": "server warm",
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    except Exception as e:
        return jsonify({
            "status": "server on",
            "warmup status": "warmup failed",
            "error": str(e)
        }), 500


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


def fetch_history(symbol, start, end, require_fast_info=False):
    try:
        print(f"[FETCH] {symbol}")

        ticker = yf.Ticker(symbol)
        df = ticker.history(
            start=start,
            end=end,
            auto_adjust=False
        ).reset_index()

        time.sleep(0.2 + random.random() * 0.3)

        if df.empty:
            return None, None

        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date", "Close"])

        # HOLIDAY EXCLUSION
        df = df[~df["Date"].dt.date.isin(HOLIDAYS)]

        fast_info = None
        if require_fast_info:
            fast_info = ticker.fast_info

        return df, fast_info

    except Exception as e:
        print(f"[ERROR] fetch_history failed for {symbol}: {e}")
        traceback.print_exc()
        return None, None


# --------------------------------------------------
# HARD DATA ENDPOINT
# --------------------------------------------------

@app.route("/hard-data", methods=["GET"])
def hard_data():
    try:
        symbol = request.args.get("symbol")
        if not symbol:
            return jsonify({"error": "symbol parameter missing"}), 400

        symbol = normalize_symbol(symbol)

        today = datetime.today().date()
        start_date = today - timedelta(days=370)
        end_date = today + timedelta(days=1)

        df, _ = fetch_history(symbol, start_date.isoformat(), end_date.isoformat())

        if df is None or df.empty:
            return jsonify({"error": "No data found"}), 404

        # EXCLUDE LTP
        df = df.iloc[:-1]

        closes = df["Close"].astype(float)

        # --------------------------------------------
        # 52W HIGH (Calendar Year Logic)
        # --------------------------------------------

        ltp_date = df["Date"].iloc[-1].date()
        one_year_back = ltp_date.replace(year=ltp_date.year - 1)

        eligible_df = df[df["Date"].dt.date >= one_year_back]

        if eligible_df.empty:
            high_52w = float(df["High"].max())
        else:
            high_52w = float(eligible_df["High"].max())

        output = {
            "symbol": symbol,
            "sum_19": sum_last(closes, 19),
            "sum_49": sum_last(closes, 49),
            "sum_99": sum_last(closes, 99),
            "sum_199": sum_last(closes, 199),
            "52w_high": high_52w,
            "data_upto": str(df["Date"].iloc[-1].date())
        }

        return jsonify({
            "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "hard_data": output
        })

    except Exception as e:
        print("[FATAL] /hard-data crashed:", e)
        traceback.print_exc()
        return jsonify({
            "error": "hard-data failed",
            "details": str(e)
        }), 500


# --------------------------------------------------
# SOFT DATA ENDPOINT
# --------------------------------------------------

@app.route("/soft-data", methods=["GET"])
def soft_data():
    try:
        symbol = request.args.get("symbol")
        if not symbol:
            return jsonify({"error": "symbol parameter missing"}), 400

        symbol = normalize_symbol(symbol)

        today = datetime.today().date()
        start_date = today - timedelta(days=10)
        end_date = today + timedelta(days=1)

        df, _ = fetch_history(symbol, start_date.isoformat(), end_date.isoformat())

        if df is None or df.empty:
            return jsonify({"error": "No data found"}), 404

        last = df.iloc[-1]

        output = {
            "symbol": symbol,
            "last_trading_date": str(last["Date"].date()),
            "close": float(last["Close"]),
            "high": float(last["High"])
        }

        return jsonify({
            "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "soft_data": output
        })

    except Exception as e:
        print("[FATAL] /soft-data crashed:", e)
        traceback.print_exc()
        return jsonify({
            "error": "soft-data failed",
            "details": str(e)
        }), 500


# --------------------------------------------------
# RUN SERVER
# --------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
