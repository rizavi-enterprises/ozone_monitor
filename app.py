from flask import Flask, render_template
import requests
import mysql.connector
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, timedelta

app = Flask(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', 'root'),
    'database': os.getenv('DB_NAME', 'weather_db')
}

# OpenWeather API configuration
OPENWEATHER_API_KEY = "819fdaba13dcbfbef6271914edd30988"  # Replace with your OpenWeather API key
CITY = "New York"

# Fetch historical ozone data from OpenWeather API
def fetch_historical_ozone_data():
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)  # Fetch data for the last 30 days
    url = f"http://api.openweathermap.org/data/2.5/air_pollution/history?lat=40.7128&lon=-74.0060&start={int(start_date.timestamp())}&end={int(end_date.timestamp())}&appid={OPENWEATHER_API_KEY}"
    response = requests.get(url)
    data = response.json()
    return data

# Save ozone data to MariaDB
def save_ozone_data_to_db(data):
    connection = None
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()
        query = """
            INSERT INTO ozone_data (city, ozone, timestamp)
            VALUES (%s, %s, %s)
        """
        for entry in data['list']:
            values = (
                CITY,
                entry['components']['o3'],  # Ozone level
                datetime.fromtimestamp(entry['dt'])  # Timestamp
            )
            cursor.execute(query, values)
        connection.commit()
    except Exception as e:
        print(f"Error saving to database: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

# Fetch ozone data from MariaDB
def fetch_ozone_data_from_db():
    connection = None
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        query = "SELECT city, ozone, timestamp FROM ozone_data ORDER BY timestamp"
        df = pd.read_sql(query, connection)
        return df
    except Exception as e:
        print(f"Error fetching data from database: {e}")
        return pd.DataFrame()
    finally:
        if connection and connection.is_connected():
            connection.close()

# Dashboard route
@app.route('/')
def dashboard():
    # Fetch historical ozone data from OpenWeather API
    historical_data = fetch_historical_ozone_data()
    save_ozone_data_to_db(historical_data)

    # Fetch ozone data from MariaDB
    ozone_data = fetch_ozone_data_from_db()

    # Create a Plotly chart
    if not ozone_data.empty:
        fig = px.line(ozone_data, x='timestamp', y='ozone', title='Historical Ozone Levels in New York')
        chart_html = fig.to_html(full_html=False)
    else:
        chart_html = "<p>No data available.</p>"

    # Render the dashboard
    return render_template('dashboard.html', chart_html=chart_html)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)