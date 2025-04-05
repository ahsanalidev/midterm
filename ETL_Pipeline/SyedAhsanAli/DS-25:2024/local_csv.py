import pandas as pd
from datetime import datetime


def transform_csv_data(csv_data):
    df_csv = pd.read_csv(csv_data)
    
    # Add the city mapping column
    
    if 'NAME' not in df_csv.columns:
        raise KeyError("'NAME' column not found in CSV. Check that the CSV has the correct header or rename columns accordingly.")
    
    df_csv['CITY'] = df_csv['NAME'].apply(lambda x: 'New York' if 'NY CITY' in x else x)
    
    # Ensure DATE is in the correct format
    df_csv['DATE'] = pd.to_datetime(df_csv['DATE'])
 
    derived_df = pd.DataFrame()
    derived_df['city'] = df_csv['CITY']
    derived_df['date_time'] = df_csv['DATE']
    
    # Use TAVG for temperature if available
    if 'TAVG' in df_csv.columns:
        derived_df['temperature'] = df_csv['TAVG']
    else:
        derived_df['temperature'] = None

    # Use AWND for wind speed if available
    if 'AWND' in df_csv.columns:
        derived_df['wind_speed'] = df_csv['AWND']
    else:
        derived_df['wind_speed'] = None
    
    # For date_time, reuse the DATE column
    if 'DATE' in df_csv.columns:
        derived_df['date_time'] = df_csv['DATE']
        derived_df['date_time'] = derived_df['date_time'].astype(str)

    else:
        derived_df['date_time'] = None

    # For humidity, pressure, and weather, there's no direct CSV column
    # so we set them to None or merge with another dataset if needed.
    derived_df['humidity'] = None
    derived_df['pressure'] = None
    derived_df['weather'] = None

    return derived_df


# if __name__ == "__main__":
#     csv_path = "/Users/ahsanali/Documents/midterm/ETL_Pipeline/SyedAhsanAli/DS-25:2024/data/new_york_weather_local.csv"
    
#     try:
#         result_df = transform_csv_data(csv_path)
#         print(result_df)
#     except FileNotFoundError:
#         print(f"Error: File not found at {csv_path}")
#     except Exception as e:
#         print(f"An error occurred: {e}")