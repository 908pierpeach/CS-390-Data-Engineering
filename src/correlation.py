import pandas as pd
import os
import yaml

def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    with open(os.path.join(project_root, "config.yaml")) as f:
        config = yaml.safe_load(f)

    df = pd.read_csv(os.path.join(project_root, "data", "processed", "returns_combined.csv"), index_col="Date")
    processed_dir = os.path.join(project_root, "data", "processed")

    stage_order = [ticker for group in config['universe'].values() for ticker in group]

    corr_matrix = df[stage_order].corr()
    corr_matrix = corr_matrix.loc[stage_order, stage_order]
    corr_matrix.to_csv(os.path.join(processed_dir, "correlation_matrix.csv"))

if __name__ == "__main__":
    main()