from flask import Flask, jsonify, request
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time
import random
import traceback
import os
import re
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# --------------------------------------------------
# STATUS + FULL WARMUP (UNCHANGED)
# --------------------------------------------------

@app.route("/status", methods=["GET"])
def status():
    try:
        print("=== FULL WARMUP START ===")

        ticker = yf.Ticker("RELIANCE.NS")

        ticker.history(period="5d")
        time.sleep(2)

        _ = ticker.fast_info

        time.sleep(1)

        pd.to_datetime(["2024-01-01", "2024-01-02"])
        pd.DataFrame({"a": [1, 2, 3]}).astype(float)

        time.sleep(2)

        print("=== FULL WARMUP COMPLETE ===")

        return jsonify({
            "status":        "server on",
            "warmup status": "server warm",
            "time":          datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    except Exception as e:
        return jsonify({
            "status":        "server on",
            "warmup status": "warmup failed",
            "error":         str(e)
        }), 500


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

HOLIDAY_FILE = "holidays.csv"
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
MERGE_DIR    = os.path.join(BASE_DIR, "merge")

os.makedirs(MERGE_DIR, exist_ok=True)

# --------------------------------------------------
# MERGE COLUMN MAP
#
# Maps DataFrame column names (yfinance casing) to
# the corresponding lowercase column names in the
# merge CSV file.
# Only these five fields can be sourced from merge files.
# --------------------------------------------------

MERGE_COLUMN_MAP = {
    "Close":  "close",
    "Open":   "open",
    "High":   "high",
    "Low":    "low",
    "Volume": "volume"
}

# Regex to match merge filenames: YYYY-MM-DD.csv
MERGE_FILENAME_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.csv$")


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

        # FIX: Strip timezone from Date immediately after yfinance returns it.
        # yfinance sometimes returns tz-aware timestamps (e.g. UTC or IST).
        # The merge rows are always tz-naive (plain pd.Timestamp).
        # Mixing tz-aware and tz-naive causes a comparison error during sort.
        # dt.tz_localize(None) removes timezone info safely — if the timestamp
        # is already tz-naive, this call has no effect.
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.tz_localize(None)

        df = df.dropna(subset=["Date", "Close"])

        # HOLIDAY EXCLUSION — merge begins only after this
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
# MERGE HELPER — apply_merge(df, symbol, start, end)
#
# Called AFTER fetch_history() returns a holiday-cleaned
# DataFrame. Scans the merge/ folder for files whose date
# falls within [fetch_start, fetch_end] inclusive.
#
# For each qualifying merge file:
#   - Checks if that date is already in the df.
#   - If already present: skips (no overwrite, no duplicate).
#   - If missing: reads the symbol's row from the merge file,
#     inserts ONLY the columns that already exist in the df,
#     re-sorts by date, and returns the corrected df.
#
# The function looks at what columns df currently has and
# only fills those from the merge file. It never adds new
# columns that weren't already in the df. This makes it
# safe to call from any future endpoint regardless of what
# columns that endpoint's df contains.
#
# Parameters:
#   df          : Holiday-cleaned DataFrame from fetch_history
#   symbol      : Normalised symbol string (e.g. RELIANCE.NS)
#   fetch_start : date object — start of the endpoint's window
#   fetch_end   : date object — end of the endpoint's window
#
# Returns:
#   Corrected DataFrame (sorted by Date), with missing dates
#   filled in from merge files where available.
# --------------------------------------------------

def apply_merge(df, symbol, fetch_start, fetch_end):

    # ---- Step 1: Scan merge folder for qualifying files ----
    # Only files whose date falls within [fetch_start, fetch_end].
    # Each endpoint passes its own window so the scan is always
    # relevant to what that endpoint actually fetched.

    qualifying_files = {}   # { date -> filepath }

    if not os.path.isdir(MERGE_DIR):
        return df

    for fname in os.listdir(MERGE_DIR):
        m = MERGE_FILENAME_RE.match(fname)
        if not m:
            continue

        date_str = m.group(1)

        try:
            file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            print(f"  [MERGE WARNING] Unparseable date in filename: {fname}. Skipping.")
            continue

        # Include only if within this endpoint's fetch window
        if fetch_start <= file_date <= fetch_end:
            qualifying_files[file_date] = os.path.join(MERGE_DIR, fname)

    if not qualifying_files:
        # No merge files relevant to this window — return df untouched
        return df

    # ---- Step 2: Collect dates already present in df ----
    # Used as a duplicate guard — we never overwrite existing data.
    existing_dates = set(df["Date"].dt.date)

    # ---- Step 3: Identify which df columns can be filled from merge ----
    # Only columns that (a) exist in the df AND (b) exist in MERGE_COLUMN_MAP.
    # This means if df only has Date, Close, High — only those are filled.
    # If df has Date, Close, Open, High, Low, Volume — all five are filled.
    # No new columns are ever added to the df.
    fillable_columns = [
        col for col in df.columns
        if col in MERGE_COLUMN_MAP
    ]

    # ---- Step 4: Process each qualifying file in date order ----
    new_rows = []

    for file_date, filepath in sorted(qualifying_files.items()):

        # Skip if df already has this date
        if file_date in existing_dates:
            print(f"  [MERGE] {file_date} already in data for {symbol}. Skipping.")
            continue

        # Read the merge file
        try:
            mdf = pd.read_csv(filepath)
        except Exception as e:
            print(f"  [MERGE WARNING] Could not read {filepath}: {e}")
            continue

        # Normalise merge file column names to lowercase
        mdf.columns = [c.strip().lower() for c in mdf.columns]

        # Must have a symbol column to match against
        if "symbol" not in mdf.columns:
            print(f"  [MERGE WARNING] No 'symbol' column in {filepath}. Skipping.")
            continue

        # Normalise symbols in the merge file for matching
        mdf["symbol"] = mdf["symbol"].apply(normalize_symbol)

        # Find the row for this symbol
        symbol_rows = mdf[mdf["symbol"] == symbol]

        if symbol_rows.empty:
            # This symbol is not in this merge file — skip silently
            continue

        # Take the first matching row
        merge_row = symbol_rows.iloc[0]

        # ---- Build new df row with only the fillable columns ----
        # Date is always tz-naive pd.Timestamp — consistent with the
        # timezone fix applied in fetch_history.
        new_entry = {"Date": pd.Timestamp(file_date)}

        for col in fillable_columns:
            merge_col = MERGE_COLUMN_MAP[col]   # e.g. "Close" -> "close"

            if merge_col not in mdf.columns:
                # Merge file doesn't have this column — use NaN
                new_entry[col] = float("nan")
                continue

            val = merge_row[merge_col]
            try:
                new_entry[col] = float(val)
            except (ValueError, TypeError):
                new_entry[col] = float("nan")

        new_rows.append(new_entry)
        print(f"  [MERGE] Inserted {file_date} for {symbol}.")

    # ---- Step 5: If any new rows were built, append and re-sort ----
    if new_rows:
        new_df = pd.DataFrame(new_rows)

        # Ensure new_df has all columns of original df.
        # Non-fillable columns (e.g. Dividends, Stock Splits) get NaN.
        for col in df.columns:
            if col not in new_df.columns:
                new_df[col] = float("nan")

        # Combine and sort — both df and new_df Date columns are
        # tz-naive at this point so the sort works without errors.
        df = pd.concat([df, new_df], ignore_index=True)
        df = df.sort_values("Date").reset_index(drop=True)

    return df


# --------------------------------------------------
# LEAP YEAR HELPERS
# Defined at module level so any future endpoint
# can use them without redefining.
# Logic completely unchanged from original.
# --------------------------------------------------

def is_leap(year):
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)


def get_lookback_days(ltp_date):
    year  = ltp_date.year
    month = ltp_date.month

    # March-Dec of leap year
    if is_leap(year) and month >= 3:
        return 366

    # Jan-Feb of year after leap year
    if is_leap(year - 1) and month <= 2:
        return 366

    return 365


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

        today      = datetime.today().date()
        start_date = today - timedelta(days=370)
        end_date   = today + timedelta(days=1)

        # Step 1: Fetch from yfinance.
        # Timezone is stripped and holidays are removed inside fetch_history.
        df, _ = fetch_history(symbol, start_date.isoformat(), end_date.isoformat())

        if df is None or df.empty:
            return jsonify({"error": "No data found"}), 404

        # Step 2: Apply merge AFTER holiday cleaning.
        # Window: [today - 370, today].
        # end_date for yfinance is today+1 (exclusive) but merge window
        # uses today as upper bound since merge files are real trading dates.
        df = apply_merge(df, symbol, start_date, today)

        # Step 3: All computations on the fully merged df.
        # Logic completely unchanged from original.

        # Exclude LTP for sums
        df_excl_ltp = df.iloc[:-1]
        closes      = df_excl_ltp["Close"].astype(float)

        # 52W High — leap aware, excludes LTP date
        ltp_date      = df["Date"].iloc[-1].date()
        lookback_days = get_lookback_days(ltp_date)
        window_start  = ltp_date - timedelta(days=lookback_days)

        window_df = df[df["Date"].dt.date >= window_start]
        window_df = window_df[window_df["Date"].dt.date < ltp_date]

        if window_df.empty:
            high_52w = None
        else:
            high_52w = float(window_df["High"].max())

        output = {
            "symbol":    symbol,
            "sum_19":    sum_last(closes, 19),
            "sum_49":    sum_last(closes, 49),
            "sum_99":    sum_last(closes, 99),
            "sum_199":   sum_last(closes, 199),
            "52w_high":  high_52w,
            "data_upto": str(df_excl_ltp["Date"].iloc[-1].date())
        }

        return jsonify({
            "as_of":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "hard_data": output
        })

    except Exception as e:
        print("[FATAL] /hard-data crashed:", e)
        traceback.print_exc()
        return jsonify({
            "error":   "hard-data failed",
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

        today      = datetime.today().date()
        start_date = today - timedelta(days=10)
        end_date   = today + timedelta(days=1)

        # Step 1: Fetch from yfinance.
        # Timezone is stripped and holidays are removed inside fetch_history.
        df, _ = fetch_history(symbol, start_date.isoformat(), end_date.isoformat())

        if df is None or df.empty:
            return jsonify({"error": "No data found"}), 404

        # Step 2: Apply merge AFTER holiday cleaning.
        # Window: [today - 10, today] — soft data's own narrow window.
        # Merge files outside this 10-day window are ignored entirely.
        df = apply_merge(df, symbol, start_date, today)

        # Step 3: Compute soft data on the merged df.
        # Logic completely unchanged from original.
        last = df.iloc[-1]

        output = {
            "symbol":            symbol,
            "last_trading_date": str(last["Date"].date()),
            "close":             float(last["Close"]),
            "high":              float(last["High"])
        }

        return jsonify({
            "as_of":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "soft_data": output
        })

    except Exception as e:
        print("[FATAL] /soft-data crashed:", e)
        traceback.print_exc()
        return jsonify({
            "error":   "soft-data failed",
            "details": str(e)
        }), 500


# --------------------------------------------------
# RUN SERVER
# --------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
