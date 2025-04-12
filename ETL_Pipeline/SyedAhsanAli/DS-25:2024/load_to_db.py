import datetime
import json
import logging
import sqlite3
import pandas as pd
import local_csv
import google_sheet
import relational_db


def transform_weather_data_openweather(data):
    """Transform the extracted weather data into a structured format."""
    try:
        if not data:
            return None

        transformed_data = {
            "city": data["name"],
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "pressure": data["main"]["pressure"],
            "weather": data["weather"][0]["description"],
            "wind_speed": data["wind"]["speed"],
            "date_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        df = pd.DataFrame([transformed_data])
        logging.info("Successfully transformed weather data.")
        return df
    except Exception as e:
        logging.error(f"Error transforming weather data: {e}")
        return None
    

def transform_weather_data_postgres(df):
    """Transform the fetched PostgreSQL weather data into a structured format similar to OpenWeather data.
    
    Args:
        df: pandas DataFrame with columns:
            city_id, city_name, temperature, humidity, weather_description, wind_speed, cloudiness, timestamp

    Returns:
        pandas DataFrame with columns:
            city, temperature, humidity, pressure, weather, wind_speed, date_time
    """
    if df.empty:
        logging.info("Input dataframe is empty.")
        return None

    transformed_data = []
    for _, row in df.iterrows():
        # Convert the Unix timestamp (assumed seconds) to a human-readable datetime string.
        date_time = datetime.datetime.fromtimestamp(row["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
        
        # Build the transformed row.
        transformed_row = {
            "city": row["city_name"],
            "temperature": row["temperature"],
            "humidity": row["humidity"],
            "pressure": None,  # No pressure data available; adjust as required.
            "weather": row["weather_description"],
            "wind_speed": row["wind_speed"],
            "date_time": date_time
        }
        transformed_data.append(transformed_row)

    new_df = pd.DataFrame(transformed_data)
    logging.info("Successfully transformed weather data into the OpenWeather format.")
    return new_df


def transform_weather_data_mongo(mongo_data_list):
  
    try:
        if not mongo_data_list:
            logging.warning("No MongoDB data to process")
            return None

        # Convert list of MongoDB documents to DataFrame
        df = pd.DataFrame(mongo_data_list)
        
        # Remove MongoDB _id field if present
        if '_id' in df.columns:
            df = df.drop('_id', axis=1)
            
     
        
        logging.info(f"Successfully processed {len(df)} MongoDB weather records")
        return df
    except Exception as e:
        logging.error(f"Error processing MongoDB data: {e}")
        return None

def load_data_to_db(df, db_name, table_name):
    """Load the transformed data into an SQLite database."""
    try:
        conn = sqlite3.connect(db_name)
        df.to_sql(table_name, conn, if_exists="append", index=False)
        conn.close()
        logging.info("Successfully loaded data into database.")
    except Exception as e:
        logging.error(f"Error loading data into database: {e}")


def load_to_db():
# load json data from /Users/ahsanali/Documents/midterm/ETL_Pipeline/SyedAhsanAli/DS-25:2024/data/New York_weather.json and apply transform_weather_data and load_data_to_db to it
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting ETL pipeline.")  
    # Load JSON data
    with open("data/New York_weather.json", "r") as json_file:
        data = json.load(json_file)
    # Transform data
    transformed_data_openweather = transform_weather_data_openweather(data)
    # Load data to database
    with open("data/New York_weather_mongo.json", "r") as json_file:
        data = json.load(json_file)
    # Transform data
    transformed_data_mongo =transform_weather_data_mongo(data)
    # Load data to database
    csv_path = "/Users/ahsanali/Documents/midterm/ETL_Pipeline/SyedAhsanAli/DS-25:2024/data/new_york_weather_local.csv"
    result_df_csv = local_csv.transform_csv_data(csv_path)
    load_google_sheet_data = google_sheet.load_google_sheet_data()
    load_relational_db_data = relational_db.fetch_all_data_df()
    transformed_data_postgres = transform_weather_data_postgres(load_relational_db_data)
    transformed_data = pd.concat([transformed_data_openweather, transformed_data_mongo, result_df_csv, load_google_sheet_data, transformed_data_postgres], ignore_index=True)
    # get the shape of the tranformed data
    transformed_data_shape = transformed_data.shape
    print(transformed_data_shape)
    try:
        load_data_to_db(transformed_data, "data/weather_data.db", "weather")
        logging.info("ETL pipeline completed successfully.")
    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}") 

if __name__ == "__main__":
    load_to_db()    