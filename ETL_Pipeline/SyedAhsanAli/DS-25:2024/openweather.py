import requests
import sqlite3
import pandas as pd
import datetime
import time
import logging
import random
import json
import os

# Set up logging
logging.basicConfig(filename="etl_pipeline.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# OpenWeatherMap API Configuration
API_KEY = "8fe779618313f842d2876f199cc65fb6"  # Replace with your API key
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# SQLite Database Configuration
#how could I update it so this weather_data.db is saved in the data folder
DATA_FOLDER = "data"
DB_NAME = os.path.join(DATA_FOLDER, "weather_data.db")


TABLE_NAME = "weather"

# List of synthetic weather conditions
WEATHER_CONDITIONS = ["Clear", "Cloudy", "Rainy", "Stormy", "Snowy", "Foggy", "Windy"]


def extract_weather_data(city):
    """Extract weather data from OpenWeatherMap API."""
    try:
        params = {"q": city, "appid": API_KEY, "units": "metric"}
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Successfully extracted weather data for {city}.")
        # save the data as json file in data folder
        if not os.path.exists(DATA_FOLDER):
            os.makedirs(DATA_FOLDER)
        with open(os.path.join(DATA_FOLDER, f"{city}_weather.json"), "w") as json_file:
            json.dump(data, json_file)
        logging.info(f"Weather data saved to {city}_weather.json.")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed. Generating synthetic data. Error: {e}")
        return generate_synthetic_data(city)


def generate_synthetic_data(city):
    """Generate synthetic weather data when API fails."""
    logging.info(f"Generating synthetic weather data for {city}.")
    return {
        "name": city,
        "main": {
            "temp": round(random.uniform(-10, 40), 2),  # Random temperature (-10 to 40°C)
            "humidity": random.randint(10, 100),  # Humidity (10-100%)
            "pressure": random.randint(980, 1050),  # Pressure (980-1050 hPa)
        },
        "weather": [{"description": random.choice(WEATHER_CONDITIONS)}],  # Random weather condition
        "wind": {"speed": round(random.uniform(0, 20), 2)},  # Wind speed (0-20 m/s)
    }