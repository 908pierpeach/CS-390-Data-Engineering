import pandas as pd
import os
import numpy as np

def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    df = pd.read_csv(os.path.join(project_root, "data", "processed", "returns_combined.csv"), index_col="Date")
    processed_dir = os.path.join(project_root, "data", "processed")


    max_results = []
    series_results = {}

    for ticker in df.columns:
            cumulative = np.exp(df[ticker].cumsum())
            running_peak = cumulative.cummax()
            drawdown = (cumulative / running_peak) - 1
            max_drawdown = drawdown.min()
            max_drawdown_date = drawdown.idxmin()
            series_results[ticker] = drawdown
            max_results.append({"ticker": ticker, "max_drawdown": max_drawdown, "date": max_drawdown_date})

    max_drawdown_df = pd.DataFrame(max_results)
    drawdown_series_df = pd.DataFrame(series_results, index = df.index)
    max_drawdown_df.to_csv(os.path.join(processed_dir, "max_drawdown.csv"), index=False)
    drawdown_series_df.to_csv(os.path.join(processed_dir, "drawdown_series.csv"))

if __name__ == "__main__":
    main()