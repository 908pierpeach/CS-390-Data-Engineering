import numpy as np
import pandas as pd
import os
import yaml
import logging

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
df = pd.read_csv(os.path.join(project_root, "data", "processed", "returns_combined.csv"), index_col="Date")

with open(df, "r") as f:
    config = yaml.safe_load(f)

correlations_dict = {}
for ticker in config["tickers"]:
    df.corr()
