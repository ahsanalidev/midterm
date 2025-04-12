import etl_pipeline
import load_to_db
if __name__ == "__main__":
    # while True:
    #         etl_pipeline.fetch_weather_data_all()
                #load_to_db.load_to_db()
    #     time.sleep(3600)  # Run every 1 hour
    etl_pipeline.fetch_weather_data_all()
    load_to_db.load_to_db()

    