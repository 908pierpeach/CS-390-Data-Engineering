import pandas as pd
import os

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
df = pd.read_csv(os.path.join(project_root, "data", "processed", "returns_combined.csv"), index_col="Date")
processed_dir = os.path.join(project_root, "data", "processed")

