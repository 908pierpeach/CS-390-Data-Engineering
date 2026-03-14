import pandas as pd
import os

def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    df = pd.read_csv(os.path.join(project_root, "data", "processed", "returns_combined.csv"), index_col="Date")
    processed_dir = os.path.join(project_root, "data", "processed")

    corr_matrix = df.corr()
    corr_matrix.to_csv(os.path.join(processed_dir, "correlation_matrix.csv"))

if __name__ == "__main__":
    main()