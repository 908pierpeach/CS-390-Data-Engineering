import pandas as pd
import os

def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    df = pd.read_csv(os.path.join(project_root, "data", "processed", "returns_combined.csv"), index_col="Date")
    processed_dir = os.path.join(project_root, "data", "processed")

    results = []

    for ticker1 in df.columns:
        for ticker2 in df.columns:
            if ticker1 != ticker2:
                for lag in range(1, 4):
                    corr = df[ticker1].corr(df[ticker2].shift(-lag))
                    results.append({"ticker1": ticker1, "ticker2": ticker2, "correlation": corr, "lag": lag})

    leadlag_df = pd.DataFrame(results)
    leadlag_df.to_csv(os.path.join(processed_dir, "leadlag_matrix.csv"), index=False)

if __name__ == "__main__":
    main()