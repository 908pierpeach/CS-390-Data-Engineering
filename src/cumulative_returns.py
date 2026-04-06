import pandas as pd
import os
import numpy as np

def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    df = pd.read_csv(os.path.join(project_root, "data", "processed", "returns_combined.csv"), index_col="Date")
    processed_dir = os.path.join(project_root, "data", "processed")

    cumulative = np.exp(df.cumsum())

    cumulative.index = pd.to_datetime(cumulative.index)
    cumulative = cumulative.reindex(
        pd.date_range(cumulative.index.min(), cumulative.index.max(), freq="D")
    ).ffill()
    cumulative.index.name = "Date"

    cumulative.to_csv(os.path.join(processed_dir, "cumulative_returns.csv"))

if __name__ == "__main__":
    main()