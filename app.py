from flask import Flask, jsonify, request
import pandas as pd
import requests
from datetime import datetime, timedelta
import os

app = Flask(__name__)

# --------------------------------------------------
# PATHS
# --------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UNIVERSE_DIR = os.path.join(BASE_DIR, "universes")
HARD_DATA_DIR = os.path.join(BASE_DIR, "hard_data")
HISTORICAL_DIR = os.path.join(BASE_DIR, "historical_data")
HOLIDAY_FILE = os.path.join(BASE_DIR, "holidays.csv")

API_BASE = "https://yfinance-data-api.onrender.com/get_stock_data_between_dates"

os.makedirs(HARD_DATA_DIR, exist_ok=True)
os.makedirs(HISTORICAL_DIR, exist_ok=True)

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

def fetch_stock_data(symbol, start, end):
    params = {
        "symbol": symbol,
        "start": start,
        "end": end,
        "fields": "Close,High,52weekhigh"
    }
    try:
        r = requests.get(API_BASE, params=params)
        if r.status_code != 200:
            return None
        data = r.json()
        if not isinstance(data, list) or len(data) < 2:
            return None
        header = data[0]
        rows = data[1:]
        df = pd.DataFrame(rows, columns=header)
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        return df.dropna(subset=["Date", "Close"])
    except:
        return None

def sum_last(series, n):
    if len(series) < n:
        return "NIL"
    return float(series.iloc[-n:].sum())

# --------------------------------------------------
# SERVER STATUS
# --------------------------------------------------

@app.route("/status", methods=["GET"])
def status():
    return jsonify({
        "status": "server on",
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

# --------------------------------------------------
# HARD DATA
# --------------------------------------------------

@app.route("/run-daily", methods=["GET"])
def run_daily():

    universe = request.args.get("universe", "").upper()
    universe_file = os.path.join(UNIVERSE_DIR, f"{universe}.csv")

    if not os.path.exists(universe_file):
        return jsonify({"error": "Universe not found"}), 400

    symbols = pd.read_csv(universe_file, header=None)[0].dropna()
    symbols = [normalize_symbol(s) for s in symbols]

    today = datetime.today().date()
    start_date = today - timedelta(days=370)
    end_date = today + timedelta(days=1)

    rows = []
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for symbol in symbols:
        df = fetch_stock_data(symbol, start_date.isoformat(), end_date.isoformat())
        if df is None or df.empty:
            continue

        df = df[~df["Date"].dt.date.isin(HOLIDAYS)]
        df = df.iloc[:-1]

        if len(df) < 20:
            continue

        closes = df["Close"].astype(float)

        rows.append({
            "symbol": symbol,
            "sum_19": sum_last(closes, 19),
            "sum_49": sum_last(closes, 49),
            "sum_99": sum_last(closes, 99),
            "sum_199": sum_last(closes, 199),
            "52w_high": df["52weekhigh"].iloc[-1]
        })

    out_file = os.path.join(HARD_DATA_DIR, f"{universe}_breadth.csv")
    with open(out_file, "w", newline="") as f:
        f.write(f"# generated_at: {generated_at}\n")
        pd.DataFrame(rows).to_csv(f, index=False)

    return jsonify({
        "universe": universe,
        "stocks_built": len(rows),
        "file_written": out_file,
        "generated_at": generated_at
    })

# (UNCHANGED IMPORTS AND SETUP ABOVE)

# --------------------------------------------------
# SOFT DATA + HISTORICAL
# --------------------------------------------------

@app.route("/breadth", methods=["GET"])
def breadth():

    universe = request.args.get("universe", "").upper()
    hard_file = os.path.join(HARD_DATA_DIR, f"{universe}_breadth.csv")

    if not os.path.exists(hard_file):
        return jsonify({"error": "Run /run-daily first"}), 400

    with open(hard_file, "r") as f:
        hard_timestamp = f.readline().replace("# generated_at:", "").strip()

    hard_df = pd.read_csv(hard_file, comment="#")

    counts = {
        "20MA": {"above": 0, "available": 0},
        "50MA": {"above": 0, "available": 0},
        "100MA": {"above": 0, "available": 0},
        "200MA": {"above": 0, "available": 0},
    }

    new_highs = 0
    high_available = 0

    today = datetime.today().date()
    start_date = today - timedelta(days=8)
    end_date = today + timedelta(days=1)

    last_trading_date = None

    for _, row in hard_df.iterrows():

        df = fetch_stock_data(
            row["symbol"],
            start_date.isoformat(),
            end_date.isoformat()
        )

        if df is None or df.empty:
            continue

        last = df.iloc[-1]
        ltp = float(last["Close"])
        high = float(last["High"])
        last_trading_date = last["Date"].date()

        ma_map = {
            "20MA": (row["sum_19"], 20),
            "50MA": (row["sum_49"], 50),
            "100MA": (row["sum_99"], 100),
            "200MA": (row["sum_199"], 200),
        }

        for ma, (sum_val, period) in ma_map.items():

            if sum_val == "NIL":
                continue

            try:
                sum_val = float(sum_val)
            except:
                continue

            ma_value = (sum_val + ltp) / period

            counts[ma]["available"] += 1
            if ltp / ma_value >= 1:
                counts[ma]["above"] += 1

        if not pd.isna(row["52w_high"]):
            high_available += 1
            if high >= float(row["52w_high"]):
                new_highs += 1

    # ----------------------------------------------
    # BUILD OUTPUT (ENHANCED)
    # ----------------------------------------------

    breadth_output = {}
    nil_found = False

    for ma, d in counts.items():
        if d["available"] == 0:
            breadth_output[ma] = {
                "above": "NIL",
                "available": 0,
                "pct": "NIL"
            }
            nil_found = True
        else:
            breadth_output[ma] = {
                "above": d["above"],
                "available": d["available"],
                "pct": d["above"] / d["available"]
            }

    if high_available == 0:
        highs_output = {
            "above": "NIL",
            "available": 0,
            "pct": "NIL"
        }
        nil_found = True
    else:
        highs_output = {
            "above": new_highs,
            "available": high_available,
            "pct": new_highs / high_available
        }

    # ----------------------------------------------
    # SAVE TO HISTORICAL (ONLY IF NO NIL)
    # ----------------------------------------------

    hist_file = os.path.join(HISTORICAL_DIR, f"{universe}_breadth.csv")

    if not nil_found and last_trading_date is not None:
        row = {
            "date": last_trading_date,
            "20MA": breadth_output["20MA"]["pct"],
            "50MA": breadth_output["50MA"]["pct"],
            "100MA": breadth_output["100MA"]["pct"],
            "200MA": breadth_output["200MA"]["pct"],
            "52W": highs_output["pct"]
        }

        if os.path.exists(hist_file):
            hist_df = pd.read_csv(hist_file)
            hist_df = hist_df[hist_df["date"] != str(last_trading_date)]
            hist_df = pd.concat([hist_df, pd.DataFrame([row])], ignore_index=True)
        else:
            hist_df = pd.DataFrame([row])

        hist_df.to_csv(hist_file, index=False)

    return jsonify({
        "hard_data_as_of": hard_timestamp,
        "ltp_as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "breadth": breadth_output,
        "new_52w_highs": highs_output
    })


# --------------------------------------------------
# RUN SERVER
# --------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
