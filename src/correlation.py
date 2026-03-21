import pandas as pd
import os

def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    df = pd.read_csv(os.path.join(project_root, "data", "processed", "returns_combined.csv"), index_col="Date")
    processed_dir = os.path.join(project_root, "data", "processed")

    stage_order = [
        "ASML", "KLAC", "LRCX", "AMAT",  # Equipment
        "TSM",  # Foundry
        "NVDA", "AVGO", "AMD", "QCOM", "TXN", "MRVL",  # Designers
        "MU", "INTC",  # IDMs
        "SMH", "SOXX", "SPY",  # Benchmarks
    ]

    corr_matrix = df[stage_order].corr()
    corr_matrix = corr_matrix.loc[stage_order, stage_order]
    corr_matrix.to_csv(os.path.join(processed_dir, "correlation_matrix.csv"))

if __name__ == "__main__":
    main()