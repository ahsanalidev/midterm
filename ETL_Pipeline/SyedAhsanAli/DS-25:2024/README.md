# Weather Data Analysis for New York City

## Description

This project is designed to collect, transform, and analyze weather data for **New York City** to uncover weather trends and patterns. By integrating data from diverse sources—real-time APIs, historical databases, relational databases, spreadsheets, and a detailed US weather institute dataset—the project creates a unified dataset for comprehensive weather analysis.

---

## Data Sources

The project leverages the following data sources to ensure a robust dataset:

- **OpenWeatherAPI**: Real-time weather data for New York City.
- **MongoDB**: Historical weather data storage.
- **Postgres DB (hosted on Neon)**: SQL-based relational weather data.
- **Google Sheets**: Additional weather data stored in spreadsheets.
- **US Weather Institute**: A comprehensive dataset with extensive parameters, including:
  - Location: `STATION`, `NAME`, `LATITUDE`, `LONGITUDE`, `ELEVATION`, `DATE`
  - Weather Metrics: 
    - `AWND` (average wind speed)
    - `PRCP` (precipitation)
    - `SNOW`, `SNWD` (snow depth)
    - `TAVG` (average temperature)
    - `TMAX` (maximum temperature)
    - `TMIN` (minimum temperature)
    - `TSUN` (sunshine duration)
  - Wind: `WDF2`, `WDF5` (wind direction), `WSF2`, `WSF5` (wind speed)
  - Weather Types: `WT01` to `WT22` (e.g., fog, rain, snow) with attributes

This variety ensures a rich and detailed dataset for analysis.

---

## Data Transformation

To enable consistent analysis, all data is cleaned and transformed into a standardized format. Each data point is structured as:

```python
transformed_data = {
    "city": data["name"],                    # Name of the city (New York City)
    "temperature": data["main"]["temp"],     # Temperature in degrees Celsius
    "humidity": data["main"]["humidity"],    # Humidity percentage
    "pressure": data["main"]["pressure"],    # Atmospheric pressure in hPa
    "weather": data["weather"][0]["description"],  # Weather condition description
    "wind_speed": data["wind"]["speed"],     # Wind speed in meters per second
    "date_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Timestamp
}