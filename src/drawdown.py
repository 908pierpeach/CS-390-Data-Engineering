import pandas as pd
import os
import numpy as np

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
df = pd.read_csv(os.path.join(project_root, "data", "processed", "returns_combined.csv"), index_col="Date")
processed_dir = os.path.join(project_root, "data", "processed")


results = []
for ticker in df.columns:
        cumulative = np.exp(df[ticker].cumsum())
        running_peak = cumulative.cummax()
        drawdown = (cumulative / running_peak) - 1
        max_drawdown = drawdown.min()
        results.append({"ticker": ticker, "max_drawdown": max_drawdown})

drawdown_df = pd.DataFrame(results)
drawdown_df.to_csv(os.path.join(processed_dir, "max_drawdown.csv"), index=False)
