import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import os

def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    processed_dir = os.path.join(project_root, "data", "processed")
    creds_path = os.path.join(project_root, "pipeline_credentials.json")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    client = gspread.authorize(creds)

    sheet = client.open("Pipeline Data Engineering")

    files = [
        ("returns_combined", "returns_combined.csv", True),
        ("correlation_matrix", "correlation_matrix.csv", True),
        ("leadlag_matrix", "leadlag_matrix.csv", False),
        ("max_drawdown", "max_drawdown.csv", False),
        ("drawdown_series", "drawdown_series.csv", True),
        ("sharpes", "sharpes.csv", False),
        ("rolling_sharpe", "rolling_sharpe.csv", True),
        ("cumulative_returns", "cumulative_returns.csv", False),
    ]

    for tab_name, filename, has_index in files:
        filepath = os.path.join(processed_dir, filename)
        df = pd.read_csv(filepath)

        df = df.fillna("")

        try:
            worksheet = sheet.worksheet(tab_name)
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=tab_name, rows=len(df) + 1, cols=len(df.columns))

        worksheet.update(
            [df.columns.values.tolist()] + df.values.tolist()
        )
        print(f"Pushed: {tab_name} ({len(df)} rows)")

if __name__ == "__main__":
    main()