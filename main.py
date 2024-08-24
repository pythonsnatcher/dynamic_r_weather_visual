import gspread
import pandas as pd
import sqlite3
from oauth2client.service_account import ServiceAccountCredentials

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
creds_dict = {
    "type": "service_account",
    "project_id": "weather-data-429210",
    "private_key_id": "af2c31cf7a66d123c4177f73f04fd01dca645a70",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCrVky4KtVU4WMl\nv82TyT/jTDyNv4Auk475CsROQgLSgIA6/EZ9fd+sIwbImEPkNw4bThe+coobn+Je\nu5lRg7yRVmXakqDCFPjq+2nV2riB0M2dBn3sgiCSxusYc1G0ieC3tpNBncy8M8ga\n/kBnAT9HRm/RHQP3so7hgRRghPE1ZmFq9buzDueqIy1TTTD89ZWnAS/sLNxefJhg\nIRMSCf1IBS/7fakNtch1WGbZr4Z1HuKXR8MajmCN1iOR1hcAHOn50uwGp78sYnel\nj8E2PC1sK1MMIGQc/k1UwG6jo1YV1k0Eu2IyXA3JukiSwBLfc2qHCw5Bq2+EGDjI\n6ChLkZllAgMBAAECggEAVYlZ9796jUuQQfJFYXhhKsqOmH14Msh74hzb7+3IluqM\nGeaEEnZaygcahd5uVmqd4kfUVsG77Rqe2ohxfF52L2CgrMPy+bGaq0Ukix0Ma9Kg\nM7pf90jnlh80kxpPOgBzbYP6dBGhenundMJlyIa43o5tmEoSBwDfj/jvAVidSvim\nTlweUrhBeT+5psdrsmHAk/Xv1Tqf9hPdnU8J9uz+5ATgjpIuB8InDlFD+yjhWHkZ\nF1FpoJaDaN69wHnYCE2TQVdbprkiPsOa9151kj+Gc/yBOMgGSvjsH7qTC9bptlz6\nzkgUHCjLum1Q/2r9jQj+9jHSB2H+O3q9j9wvPpZr6QKBgQDdYBVzqUee7r6sngY5\nDeKhBeIMpiHt4P4UMV0MmBQEaoRHm9LZSELlYQ/Btye2NDl4fIIByYHeEYEl6Chd\nxZtXJ1Z04P4txLmb0T4m+4/BVWwztwXklrO09Kgm62lYhbtqYbJfHJY4H41Xfqk9\nIePRkXkoTWoV7hA/Q4O0dVYVQwKBgQDGIqy6iNPBN3KQZmYDG9WKuFMsAx/gSor1\nqL5duBxUNyhkpVCLTtQvdS3aElvGCjMpWKTqmSxm8s74EQ7qLVU2YIq6u+avV47e\nN01qtrKtISFDQI/7RKKEhwoYoQEAMpT3/NpUEO4hoWdtEw50h4lmi37mFeZy2PQr\ns8sVt+xYNwKBgQCkWMbUPSIsvbXU1ORtyv8q6AEvvs6FmXlHaHZZ+TUzKhjWSLq6\nEMmJHQvjlqPmwtK/vj+OMBk30er9R2NgammuxEeNMdPCCsB5C1iG/E93CoHvyrqX\nP8JeXxvO+QoWbAH9MlaIAeML+3ClOiVOezB0zvkRkJdnfHuXW/oVKN8lnQKBgCfT\nTm7ME+wxbfiybGzRinGwrR8anayirx3DxkfmOuN+lsLsK61ksee8IPRFXmcHI9N6\nuuNg2Hj08z8PhrTxWcBtVVVFcY/rBI+MBCagBHgiQaJX9tjlqdkDn7blneLhR+o0\ny9m78XGXFMfq3av0llyjS2WKH2EUVLf4EqkR6BKvAoGAIE02RZ6Cj5fJa7WeTh/f\na1PV0pEzH4A44tOM68C6aDimDUdm37WLCR/5fmH+DT4IHljzg2rWSSzpD0rWMA2G\neScy6Qs0efX8BuasmZHzg15DuiAiZqvUFoejt9gsFQ0lxd42IZOiukCX1rn/ghXF\nacrHmWIWcgU31Uo0Z+Hyxws=\n-----END PRIVATE KEY-----\n",
    "client_email": "weather-data@weather-data-429210.iam.gserviceaccount.com",
    "client_id": "117847726458310784712",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/weather-data%40weather-data-429210.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}
logging.info("Authorizing Google Sheets API...")
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
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
