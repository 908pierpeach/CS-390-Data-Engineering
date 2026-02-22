import numpy as np
import pandas as pd
import os
import yaml
import logging
from datetime import datetime

log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "logs")
os.makedirs(log_dir, exist_ok=True)

logger = logging.getLogger("transform")
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

log_filename = os.path.join(log_dir, f"transform_{datetime.now().strftime('%Y-%m-%d _%H-%M')}.log")
file_handler = logging.FileHandler(log_filename)
file_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_path = os.path.join(project_root, "config.yaml")

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

returns_dict = {}

for stage, tickers in config["universe"].items():
    for ticker in tickers:
        try:
            df = pd.read_csv(os.path.join(project_root, "data", "raw", f"{ticker}.csv"))
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.set_index("Date")
            close = pd.to_numeric(df["Close"])
            log_returns = np.log(close / close.shift(1))
            returns_dict[ticker] = log_returns
            logger.info(f"{ticker}: Computed {len(log_returns)} log returns")
        except Exception as e:
            logger.error(f"{ticker}: Failed - {e}")


combined_df = pd.DataFrame(returns_dict)
logger.info(f"{len(combined_df)} rows combined")

processed_dir = os.path.join(project_root, "data", "processed")
os.makedirs(processed_dir, exist_ok=True)
combined_df.to_csv(os.path.join(processed_dir, "returns_combined.csv"))