import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

# Define your credentials JSON file and authorize

def load_google_sheet_data():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file("/Users/ahsanali/Documents/midterm/ETL_Pipeline/SyedAhsanAli/DS-25:2024/config/testingpyanomaly-7a32896b3caa.json", scopes=scope)
    client = gspread.authorize(creds)

    # Open a worksheet by name (e.g., "Sheet1") in a specific spreadsheet
    spreadsheet = client.open("weatherdatamidterm")
    all_sheets = spreadsheet.worksheets()
    print("Available sheets:", [sheet.title for sheet in all_sheets])

    worksheet = spreadsheet.worksheet("Sheet1")

    # Extract all data into a list of lists
    data = worksheet.get_all_values()

    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(data[1:], columns=data[0])


if __name__ == "__main__":
    load_google_sheet_data()