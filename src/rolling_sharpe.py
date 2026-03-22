import pandas as pd
import os
import numpy as np

def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    df = pd.read_csv(os.path.join(project_root, "data", "processed", "returns_combined.csv"), index_col="Date")
    processed_dir = os.path.join(project_root, "data", "processed")

    simple_returns = np.exp(df) - 1
    rolling_mean = simple_returns.rolling(252).mean() * 252
    rolling_std = simple_returns.rolling(252).std() * np.sqrt(252)

    rolling_sharpe = rolling_mean / rolling_std

    rolling_sharpe = rolling_sharpe.dropna()
    rolling_sharpe.index = pd.to_datetime(rolling_sharpe.index)
    rolling_sharpe = rolling_sharpe.reindex(
        pd.date_range(rolling_sharpe.index.min(), rolling_sharpe.index.max(), freq="D")
    ).ffill()
    rolling_sharpe.index.name = "Date"

    rolling_sharpe.to_csv(os.path.join(processed_dir, "rolling_sharpe.csv"))

if __name__ == "__main__":
    main()

