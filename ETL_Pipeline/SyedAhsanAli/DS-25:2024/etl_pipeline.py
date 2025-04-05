import openweather
import mongodb
CITY = "New York"

   # Connect to MongoDB
  
    
    # Fetch all New York weather data

# def run_etl():
#     """Run the ETL pipeline."""
#     logging.info("ETL pipeline started.")
#     data = extract_weather_data(CITY)
#     transformed_data = transform_weather_data(data)
#     if transformed_data is not None:
#         load_data_to_db(transformed_data, DB_NAME, TABLE_NAME)
#     logging.info("ETL pipeline completed.")


if __name__ == "__main__":
    openweather.extract_weather_data(CITY)
    client = mongodb.connect_to_mongodb()
    if not client:
        print("❌ Failed to connect to MongoDB. Exiting.")
        exit(1)
    weather_data = mongodb.fetch_weather_data(client, city="New York")
    if not weather_data:
        print("❌ No weather data found for New York.")
        exit(1)
    
  
    print("\n🔍 Fetching the 3 most recent weather records:")
    mongodb.fetch_weather_data(client, limit=3)
    # while True:
    #     extract_weather_data(CITY)
    #     time.sleep(3600)  # Run every 1 hour