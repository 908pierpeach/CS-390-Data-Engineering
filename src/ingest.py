import yaml
import os

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_path = os.path.join(project_root, "config.yaml")

# Load the config
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# Print what we loaded
for stage, tickers in config["universe"].items():
    print(f"{stage}: {tickers}")