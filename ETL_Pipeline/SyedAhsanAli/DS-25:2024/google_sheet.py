import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

# Define your credentials JSON file and authorize

def load_google_sheet_data():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file("/Users/ahsanali/Documents/midterm/ETL_Pipeline/SyedAhsanAli/DS-25:2024/config/credentials.json", scopes=scope)
    client = gspread.authorize(creds)

    # Open a worksheet by name (e.g., "Sheet1") in a specific spreadsheet
    spreadsheet = client.open("Weather Data")
    worksheet = spreadsheet.worksheet("Sheet1")

    # Extract all data into a list of lists
    data = worksheet.get_all_values()

    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(data[1:], columns=data[0])
    print(df.head())