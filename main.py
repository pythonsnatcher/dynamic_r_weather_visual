import gspread
import pandas as pd
import sqlite3
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import logging

# Define the paths
database_file_path = 'data/weather_data.db'
spreadsheet_url = 'https://docs.google.com/spreadsheets/d/156isq2fMNww4VpcRasL6u7o8EZQC7OEZxQllLP4H8pY/edit?gid=0#gid=0'
worksheet_name = 'Sheet1'


logging.info(f"Working directory: {os.getcwd()}")

# Check if database file exists
if not os.path.exists(database_file_path):
    logging.error(f"Database file not found at {database_file_path}")
else:
    logging.info(f"Database found at {database_file_path}")



# Set up Google Sheets credentials
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

try:
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS environment variable not found")
    
    creds_dict = json.loads(creds_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    logging.info("Google Sheets API credentials loaded")
except Exception as e:
    logging.error(f"Error loading credentials: {e}")
    raise

logging.info("Authorizing Google Sheets API...")

client = gspread.authorize(creds)
sheet = client.open_by_url(spreadsheet_url)
worksheet = sheet.worksheet(worksheet_name)

logging.info("Reading data from Google Sheets...")
data = worksheet.get_all_records()
df = pd.DataFrame(data)
logging.info(f"Data retrieved from Google Sheets, total rows: {len(df)}")

# Connect to the SQLite database
logging.info("Connecting to SQLite database...")
try:
    conn = sqlite3.connect(database_file_path)
    cursor = conn.cursor()
    logging.info("Database connection successful.")
except Exception as e:
    logging.error(f"Error connecting to database: {e}")
    raise

# Process each row of the DataFrame
for _, row in df.iterrows():
    # Handle WeatherConditions
    cursor.execute('''
        INSERT OR IGNORE INTO WeatherConditions (description)
        VALUES (?)
    ''', (row['Weather Condition'],))

    cursor.execute('''
        SELECT weather_condition_id FROM WeatherConditions WHERE description = ?
    ''', (row['Weather Condition'],))
    weather_condition_id = cursor.fetchone()[0]

    # Handle WindDirections
    cursor.execute('''
        INSERT OR IGNORE INTO WindDirections (description)
        VALUES (?)
    ''', (row['Wind Direction'],))

    cursor.execute('''
        SELECT wind_direction_id FROM WindDirections WHERE description = ?
    ''', (row['Wind Direction'],))
    wind_direction_id = cursor.fetchone()[0]

    # Handle UVIndexLevels
    cursor.execute('''
        INSERT OR IGNORE INTO UVIndexLevels (level)
        VALUES (?)
    ''', (row['UV Index'],))

    cursor.execute('''
        SELECT uv_index_id FROM UVIndexLevels WHERE level = ?
    ''', (row['UV Index'],))
    uv_index_id = cursor.fetchone()[0]

    # Handle PollenLevels
    cursor.execute('''
        INSERT OR IGNORE INTO PollenLevels (level)
        VALUES (?)
    ''', (row['Pollen'],))

    cursor.execute('''
        SELECT pollen_id FROM PollenLevels WHERE level = ?
    ''', (row['Pollen'],))
    pollen_id = cursor.fetchone()[0]

    # Handle PollutionLevels
    cursor.execute('''
        INSERT OR IGNORE INTO PollutionLevels (level)
        VALUES (?)
    ''', (row['Pollution'],))

    cursor.execute('''
        SELECT pollution_id FROM PollutionLevels WHERE level = ?
    ''', (row['Pollution'],))
    pollution_id = cursor.fetchone()[0]

    # Handle VisibilityLevels
    cursor.execute('''
        INSERT OR IGNORE INTO VisibilityLevels (description)
        VALUES (?)
    ''', (row['Visibility'],))

    cursor.execute('''
        SELECT visibility_id FROM VisibilityLevels WHERE description = ?
    ''', (row['Visibility'],))
    visibility_id = cursor.fetchone()[0]

    # Handle Locations
    cursor.execute('''
        INSERT OR IGNORE INTO Locations (name)
        VALUES (?)
    ''', (row['Location'],))

    cursor.execute('''
        SELECT location_id FROM Locations WHERE name = ?
    ''', (row['Location'],))
    location_id = cursor.fetchone()[0]

    # Check if the time_of_search already exists
    cursor.execute('''
        SELECT COUNT(*) FROM WeatherReports WHERE time_of_search = ?
    ''', (row['Time of Search'],))
    if cursor.fetchone()[0] == 0:
        # Prepare values for insertion into WeatherReports
        values = (
            row['Time of Search'], row['High Temperature(°C)'], row['Low Temperature(°C)'], row['Current Temperature(°C)'],
            weather_condition_id, row['Wind Speed(mph)'], row['Humidity(%)'], row['Pressure(mb)'], visibility_id,
            location_id, wind_direction_id, uv_index_id, pollen_id, pollution_id,
            row['Chance of Precipitation(%)'], row['Sunset'], row['Sunrise'],
            row['Low Tide Morning Time'], row['Low Tide Morning Height(M)'],
            row['High Tide Morning Time'], row['High Tide Morning Height(M)'],
            row['Low Tide Evening Time'], row['Low Tide Evening Height(M)'],
            row['High Tide Evening Time'], row['High Tide Evening Height(M)']
        )

        # Insert data into WeatherReports
        cursor.execute('''
            INSERT INTO WeatherReports (
                time_of_search, high_temperature, low_temperature, current_temperature,
                weather_condition_id, wind_speed, humidity, pressure, visibility_id,
                location_id, wind_direction_id, uv_index_id, pollen_id, pollution_id,
                chance_of_precipitation, sunset, sunrise, low_tide_morning_time,
                low_tide_morning_height, high_tide_morning_time, high_tide_morning_height,
                low_tide_evening_time, low_tide_evening_height, high_tide_evening_time,
                high_tide_evening_height
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', values)

# Commit changes and close the connection
conn.commit()
conn.close()

print("Database updated successfully.")
