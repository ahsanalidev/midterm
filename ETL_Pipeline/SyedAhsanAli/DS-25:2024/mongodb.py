# Import necessary libraries
import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import certifi
import os
import json
import logging
DATA_FOLDER = "data"

def connect_to_mongodb():
    """Connect to MongoDB and return the client"""
    # Your MongoDB connection string
    uri = "mongodb+srv://bdaneduet2025:bdaneduet2025@cluster0.jpmz1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    
    # Create a new client using certifi for TLS certificate verification
    client = MongoClient(uri, server_api=ServerApi('1'), tlsCAFile=certifi.where())
    
    # Test the connection
    try:
        client.admin.command('ping')
        print("✅ Successfully connected to MongoDB!")
        return client
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None

def fetch_weather_data(client, limit=None, city=None):
    """Fetch weather data from MongoDB with optional filters"""
    try:
        # Access the database and collection
        db = client["weather_database"]
        collection = db["weather_data"]
        
        # Build query based on parameters
        query = {}
        if city:
            query["city"] = city
        
        # Fetch data
        cursor = collection.find(query).sort("date_time", -1)  # Sort by date, most recent first
        
        # Apply limit if specified
        if limit:
            cursor = cursor.limit(limit)
        
        # Convert cursor to list
        weather_data = list(cursor)
        
        print(f"📊 Retrieved {len(weather_data)} weather records")
          # save the data as json file in data folder
        if not os.path.exists(DATA_FOLDER):
            os.makedirs(DATA_FOLDER)
        with open(os.path.join(DATA_FOLDER, f"{city}_weather_mongo.json"), "w") as json_file:
            json.dump(weather_data, json_file)
        logging.info(f"Weather data saved to {city}_weather_mongo.json.")
        return weather_data
        
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return None
