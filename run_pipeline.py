import subprocess
import sys
import os

project_root = os.path.dirname(os.path.abspath(__file__))

steps = [
    ("Ingest",      os.path.join("src", "ingest.py")),
    ("Transform",   os.path.join("src", "transform.py")),
    ("Correlation", os.path.join("src", "correlation.py")),
    ("Lead-Lag",    os.path.join("src", "lead_lag.py")),
    ("Drawdown",    os.path.join("src", "drawdown.py")),
    ("Sharpe",      os.path.join("src", "sharpe.py")),
    ("Rolling Sharpe", os.path.join("src", "rolling_sharpe.py")),
    ("Cumulative Returns", os.path.join("src", "cumulative_returns.py"))
    ("Sheets Push", os.path.join("src", "push_to_sheets.py")),
]

def main():
    failed = False
    for name, script in steps:
        print(f"\n{'='*40}")
        print(f"Running: {name}")
        print(f"{'='*40}")

        result = subprocess.run(
            [sys.executable, script],
            cwd=project_root
        )

        if result.returncode != 0:
            print(f"FAILED: {name} (exit code {result.returncode})")
            failed = True
            break

        print(f"DONE: {name}")

    if failed:
        print("\nPipeline stopped due to failure.")
        sys.exit(1)
    else:
        print("\nPipeline complete.")

if __name__ == "__main__":
    main()