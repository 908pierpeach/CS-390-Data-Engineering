import yaml
import os
import pandas as pd
import yfinance as yf
import logging
from datetime import datetime

log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "logs")
os.makedirs(log_dir, exist_ok=True)

logger = logging.getLogger("ingest")
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

log_filename = os.path.join(log_dir, f"ingest_{datetime.now().strftime('%Y-%m-%d _%H-%M')}.log")
file_handler = logging.FileHandler(log_filename)
file_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_path = os.path.join(project_root, "config.yaml")

# Load the config
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

start_date = config["ingestion"]["start_date"]
if config["ingestion"]["end_date"] == "today":
    end_date = datetime.today().strftime('%Y-%m-%d')
else:
    end_date = config["ingestion"]["end_date"]

logger.debug(f"start_date: {start_date}")
logger.debug(f"end_date: {end_date}")

def fetch_ticker(ticker, start_date, end_date):
    """
    Fetch daily price and volume data for a single ticker.
    Returns a DataFrame with Date, Open, High, Low, Close.
    Returns None if the fetch fails.
    """
    try:
        logger.info(f"Fetching {ticker}...")
        df = yf.download(ticker, start=start_date, end=end_date, auto_adjust = True, progress=False)

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        if df.empty:
            logger.warning(f"{ticker}: No data returned")
            return None

        # Keep only the columns we need
        df = df[["Open", "High", "Low", "Close"]].copy()
        df.index.name = "Date"
        df = df.reset_index()

        logger.info(f"{ticker}: Got {len(df)} rows")
        return df

    except Exception as e:
        logger.error(f"{ticker}: FAILED - {e}")
        return None

def save_or_append_ticker(ticker, df):
    """
    Save a ticker's DataFrame to data/raw/{TICKER}.csv
    """
    # Create the data/raw folder if it doesn't exist
    raw_data_dir = os.path.join(project_root, "data", "raw")
    os.makedirs(raw_data_dir, exist_ok=True)
    filepath = os.path.join(raw_data_dir, f"{ticker}.csv")

    if os.path.exists(filepath):
        df_old = pd.read_csv(filepath)
        logger.debug(f"{ticker}: Loaded {len(df_old)} rows")

        df_old['Date'] = pd.to_datetime(df_old['Date'])
        df['Date'] = pd.to_datetime(df['Date'])

        df_combined = pd.concat([df_old, df], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset='Date', keep='last')
        df_combined = df_combined.sort_values('Date')
        df_combined['Date'] = df_combined['Date'].dt.strftime('%Y-%m-%d')
        logger.debug(f"{ticker}: Saving {len(df_combined)} rows")

        df_combined.to_csv(filepath, index=False)
        logger.info(f"{ticker}: Appended {len(df)} new rows (total: {len(df_combined)})")

    else:
        df.to_csv(filepath, index=False)
        logger.info(f"{ticker}: Saved {len(df)} rows (new file)")

for stage, tickers in config["universe"].items():
    for ticker in tickers:
        filepath = os.path.join(project_root, "data", "raw", f"{ticker}.csv")

        #Takes the old date's data, converts it to datetime format for manipulation, then reconverts it to string for yf.download to work
        if os.path.exists(filepath):
            df_old = pd.read_csv(filepath)
            latest_date = pd.to_datetime(df_old['Date']).max()
            fetch_start = (latest_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            fetch_start = start_date

        logger.debug(f"{ticker}: Fetching range {fetch_start} to {end_date}")
        df = fetch_ticker(ticker, fetch_start, end_date)

        if df is not None and not df.empty:
            save_or_append_ticker(ticker, df)
        else:
            logger.info(f"{ticker}: Skipping (no new data)")