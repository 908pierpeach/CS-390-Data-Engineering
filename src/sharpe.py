import pandas as pd
import os
import numpy as np

def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    df = pd.read_csv(os.path.join(project_root, "data", "processed", "returns_combined.csv"), index_col="Date")
    processed_dir = os.path.join(project_root, "data", "processed")

    sharpes = []

    for ticker in df.columns:
        simple_returns = np.exp(df[ticker]) - 1
        mean = simple_returns.mean()
        std = simple_returns.std()
        ann_mean = mean * 252
        ann_std = std * np.sqrt(252)
        sharpe = ann_mean / ann_std
        sharpes.append({"ticker": ticker, "ann_return": ann_mean, "ann_volatility": ann_std, "sharpe": sharpe})

    sharpes_df = pd.DataFrame(sharpes)
    sharpes_df.to_csv(os.path.join(processed_dir, "sharpes.csv"), index=False)

if __name__ == "__main__":
    main()