from flask import Flask, jsonify, request
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time
import random
import os
import traceback

app = Flask(__name__)

# --------------------------------------------------
# HELPERS
# --------------------------------------------------

def normalize_symbol(symbol):
    symbol = str(symbol).strip().upper()
    if symbol.endswith(".NS") or symbol.endswith(".BO"):
        return symbol
    return symbol + ".NS"

def sum_last(series, n):
    if len(series) < n:
        return "NIL"
    return float(series.iloc[-n:].sum())

def fetch_stock_data(symbol, start, end):
    try:
        print(f"[FETCH] {symbol} {start} → {end}")

        ticker = yf.Ticker(symbol)
        df = ticker.history(start=start, end=end, auto_adjust=False).reset_index()

        # micro sleep to look human
        time.sleep(0.2 + random.random() * 0.3)

        if df.empty:
            print(f"[WARN] No data for {symbol}")
            return None

        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

        fast_info = ticker.fast_info

        if "52weekhigh" not in df.columns:
            df["52weekhigh"] = fast_info.get("yearHigh", None)

        return df.dropna(subset=["Date", "Close"])

    except Exception as e:
        print(f"[ERROR] fetch_stock_data failed for {symbol}: {e}")
        traceback.print_exc()
        return None

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
# RUN DAILY (HARD DATA - IN MEMORY)
# --------------------------------------------------

@app.route("/run-daily", methods=["GET"])
def run_daily():
    try:
        universe = request.args.get("universe", "").upper()
        universe_file = os.path.join("universes", f"{universe}.csv")

        if not universe:
            return jsonify({"error": "universe parameter missing"}), 400

        if not os.path.exists(universe_file):
            return jsonify({"error": "Universe not found"}), 400

        symbols = pd.read_csv(universe_file, header=None)[0].dropna()
        symbols = [normalize_symbol(s) for s in symbols]

        print(f"[RUN-DAILY] Universe={universe}, Symbols={len(symbols)}")

        today = datetime.today().date()
        start_date = today - timedelta(days=370)
        end_date = today + timedelta(days=1)

        rows = []
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for i, symbol in enumerate(symbols, start=1):
            print(f"[RUN-DAILY] {i}/{len(symbols)} → {symbol}")

            df = fetch_stock_data(symbol, start_date.isoformat(), end_date.isoformat())
            if df is None or df.empty:
                continue

            df = df.iloc[:-1]  # drop possibly incomplete candle

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

        print(f"[RUN-DAILY] Completed. Stocks built={len(rows)}")

        return jsonify({
            "universe": universe,
            "generated_at": generated_at,
            "stocks_built": len(rows),
            "hard_data": rows
        })

    except Exception as e:
        print("[FATAL] /run-daily crashed:", e)
        traceback.print_exc()
        return jsonify({
            "error": "run-daily failed",
            "details": str(e)
        }), 500

# --------------------------------------------------
# BREADTH (SOFT DATA - JSON INPUT)
# --------------------------------------------------

@app.route("/breadth", methods=["POST"])
def breadth():
    try:
        payload = request.get_json()
        if not payload:
            return jsonify({"error": "JSON body missing"}), 400

        hard_data = payload.get("hard_data")
        if not hard_data:
            return jsonify({"error": "hard_data missing"}), 400

        counts = {
            "20MA": {"above": 0, "available": 0},
            "50MA": {"above": 0, "available": 0},
            "100MA": {"above": 0, "available": 0},
            "200MA": {"above": 0, "available": 0},
        }

        new_highs = 0
        high_available = 0
        last_trading_date = None

        today = datetime.today().date()
        start_date = today - timedelta(days=8)
        end_date = today + timedelta(days=1)

        for i, row in enumerate(hard_data, start=1):
            symbol = row.get("symbol")
            print(f"[BREADTH] {i}/{len(hard_data)} → {symbol}")

            df = fetch_stock_data(
                symbol,
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

            if row.get("52w_high") is not None:
                high_available += 1
                if high >= float(row["52w_high"]):
                    new_highs += 1

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

        return jsonify({
            "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "last_trading_date": str(last_trading_date),
            "breadth": breadth_output,
            "new_52w_highs": highs_output
        })

    except Exception as e:
        print("[FATAL] /breadth crashed:", e)
        traceback.print_exc()
        return jsonify({
            "error": "breadth failed",
            "details": str(e)
        }), 500

# --------------------------------------------------
# RUN SERVER
# --------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
