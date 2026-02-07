import yaml
import os
import pandas as pd
import yfinance as yf

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_path = os.path.join(project_root, "config.yaml")

# Load the config
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

def fetch_ticker(ticker, start_date, end_date):
    """
    Fetch daily price and volume data for a single ticker.
    Returns a DataFrame with Date, Open, High, Low, Close, Volume.
    Returns None if the fetch fails.
    """
    try:
        print(f"Fetching {ticker}...")
        df = yf.download(ticker, start=start_date, end=end_date, auto_adjust = True, progress=False)

        if df.empty:
            print(f"  {ticker}: No data returned")
            return None

        # Keep only the columns we need
        df = df[["Open", "High", "Low", "Close"]].copy()
        df.index.name = "Date"
        df = df.reset_index()

        print(f"  {ticker}: Got {len(df)} rows")
        return df

    except Exception as e:
        print(f"  {ticker}: FAILED - {e}")
        return None


def save_ticker(ticker, df):
    """
    Save a ticker's DataFrame to data/raw/{TICKER}.csv
    """
    # Create the data/raw folder if it doesn't exist
    raw_data_dir = os.path.join(project_root, "data", "raw")
    os.makedirs(raw_data_dir, exist_ok=True)

    filepath = os.path.join(raw_data_dir, f"{ticker}.csv")
    df.to_csv(filepath, index=False)
    print(f"  {ticker}: Saved to {filepath}")


for stage, tickers in config["universe"].items():
    for ticker in tickers:
        df = fetch_ticker(ticker, "2020-01-01", "2026-01-31")

        if df is not None:
            save_ticker(ticker, df)
        else:
            print(f"  {ticker}: Skipping (no data)")
