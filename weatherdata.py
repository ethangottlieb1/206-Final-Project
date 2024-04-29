import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import time

def setup_database(db_name):
    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()

    create_teams_sql = """
    CREATE TABLE IF NOT EXISTS Teams (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE
    );
    """
    create_games_sql = """
    CREATE TABLE IF NOT EXISTS Games (
        id INTEGER PRIMARY KEY,
        date TEXT,
        home_team_id INTEGER,
        away_team_id INTEGER,
        home_score INTEGER,
        away_score INTEGER,
        FOREIGN KEY (home_team_id) REFERENCES Teams (id),
        FOREIGN KEY (away_team_id) REFERENCES Teams (id),
        UNIQUE(date, home_team_id, away_team_id)
    );
    """
    create_locations_sql = """
    CREATE TABLE IF NOT EXISTS Locations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        team_name TEXT NOT NULL,
        stadium_location TEXT NOT NULL,
        UNIQUE(team_name, stadium_location)
    );
    """
    create_weather_sql = """
    CREATE TABLE IF NOT EXISTS Weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        location TEXT NOT NULL,
        date TEXT NOT NULL,
        temperature REAL,
        UNIQUE(location, date)
    );
    """
    create_metadata_sql = """
    CREATE TABLE IF NOT EXISTS Metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        last_inserted_index INTEGER DEFAULT 0
    );
    """

    cursor.execute(create_teams_sql)
    cursor.execute(create_games_sql)
    cursor.execute(create_locations_sql)
    cursor.execute(create_weather_sql)
    cursor.execute(create_metadata_sql)

    cursor.execute("INSERT OR IGNORE INTO Metadata (id, last_inserted_index) VALUES (1, 0)")

    connection.commit()
    connection.close()
    print("Database setup complete.")

def scrape_mlb_data(url):
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    mlb_table = soup.find('table', class_='sortable')
    mlb_data = []
    if mlb_table:
        for row in mlb_table.find_all('tr')[1:]:
            cols = row.find_all('td')
            if len(cols) > 1:
                team_name = cols[0].text.strip()
                stadium_location = cols[1].text.strip() + ', ' + cols[2].text.strip()
                mlb_data.append((team_name, stadium_location))
    return mlb_data

def insert_into_database(conn, mlb_data):
    c = conn.cursor()
    c.executemany("""
        INSERT OR IGNORE INTO Locations (team_name, stadium_location)
        VALUES (?, ?)
        """, mlb_data)
    conn.commit()
    print(f"Inserted {len(mlb_data)} rows into the database.")
    c.close()

def get_average_temperature(location, start_time, end_time):
    api_key = '5d1fd2c6aae64f38bf14b05574736c95'
    url = 'https://api.oikolab.com/weather'

    response = requests.get(url,
                            params={'param': ['temperature'],
                                    'location': [location],
                                    'start': start_time,
                                    'end': end_time},
                            headers={'api-key': api_key}
                            )

    if response.status_code == 200:
        data = response.json()
        nested_data = json.loads(data['data'])
        temperature_data = nested_data['data']
        temperatures = [entry[-1] for entry in temperature_data]
        average_temperature = sum(temperatures) / len(temperatures)
        return average_temperature
    else:
        print("Error:", response.status_code, response.text)

def insert_weather_into_database(conn, weather_data):
    c = conn.cursor()
    try:
        c.executemany("""
            INSERT OR IGNORE INTO Weather (location, date, temperature)
            VALUES (?, ?, ?)
            """, weather_data)
        conn.commit()
        print(f"Inserted {len(weather_data)} rows into the database.")
    except sqlite3.IntegrityError:
        print("Error: Integrity constraint violated. Skipping insertion of duplicate data.")

def fetch_and_store_weather(conn, locations, weeks, max_items=25):
    for location in locations:
        print("Storing weather data for", location)
        for i, week_start in enumerate(weeks):
            week_end = (datetime.strptime(week_start, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")
            start_time = datetime.strptime(week_start, "%Y-%m-%d").strftime("%Y-%m-%d 09:00 UTC")
            end_time = datetime.strptime(week_end, "%Y-%m-%d").strftime("%Y-%m-%d 03:00 UTC")
            average_temp = get_average_temperature(location, start_time, end_time)
            weather_data = [(location, week_start, average_temp)]
            insert_weather_into_database(conn, weather_data)
            if len(weather_data) >= max_items:
                break

def main():
    db_name = "mlb_data.db"
    url = 'https://geojango.com/pages/list-of-mlb-teams'
    locations = [
        "Phoenix, Arizona", "Cumberland, Georgia", "Baltimore, Maryland", 
        "Boston, Massachusetts", "Chicago, Illinois", "Cincinnati, Ohio", 
        "Cleveland, Ohio", "Denver, Colorado", "Detroit, Michigan", 
        "Houston, Texas", "Kansas City, Missouri", "Anaheim, California", 
        "Los Angeles, California", "Miami, Florida", "Milwaukee, Wisconsin", 
        "Minneapolis, Minnesota", "Queens, New York", "Oakland, California", 
        "Philadelphia, Pennsylvania", "Pittsburgh, Pennsylvania", 
        "San Diego, California", "San Francisco, California", "Seattle, Washington", 
        "St. Louis, Missouri", "St. Petersburg, Florida", "Arlington, Texas", 
        "Toronto, Ontario, Canada", "Washington, D.C.", "Bronx, NY"
    ]
    start_date = datetime(2023, 9, 1)
    end_date = datetime(2023, 9, 30)
    weeks = [(start_date + timedelta(weeks=i)).strftime("%Y-%m-%d") for i in range(0, 4)]

    setup_database(db_name)
    mlb_data = scrape_mlb_data(url)

    conn = sqlite3.connect(db_name)
    insert_into_database(conn, mlb_data)
    fetch_and_store_weather(conn, locations, weeks)
    conn.close()

if __name__ == "__main__":
    main()
