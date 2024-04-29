import requests
from bs4 import BeautifulSoup
import sqlite3

# URL
url = 'https://geojango.com/pages/list-of-mlb-teams'

# create tables
def setup_database(db_name):
    # Connect to SQLite database
    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()

    create_locations_sql = """
    CREATE TABLE IF NOT EXISTS Locations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        team_name TEXT NOT NULL,
        stadium_location TEXT NOT NULL,
        UNIQUE(team_name, stadium_location)
    );
    """
    create_metadata_sql = """
    CREATE TABLE IF NOT EXISTS Metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        last_inserted_index INTEGER DEFAULT 0
    );
    """

    drop_mlb_teams_sql = "DROP TABLE IF EXISTS MLBTeams;"
    drop_mlb_locations_sql = "DROP TABLE IF EXISTS MLBLocations;"
    drop_mlb_locatinons_sql = "DROP TABLE IF EXISTS MLBLocatinons;"

    # SQL commands
    cursor.execute(create_locations_sql)
    cursor.execute(create_metadata_sql)
    cursor.execute(drop_mlb_teams_sql)
    cursor.execute(drop_mlb_locations_sql)
    cursor.execute(drop_mlb_locatinons_sql)

    cursor.execute("INSERT OR IGNORE INTO Metadata (id, last_inserted_index) VALUES (1, 0)")

    # Commit
    connection.commit()
    connection.close()
    print("Database setup complete.")

# Scrape the MLB data
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

# Insert into db
def insert_into_database(db_name, mlb_data):
    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()

    cursor.execute("SELECT last_inserted_index FROM Metadata WHERE id = 1")
    last_index = cursor.fetchone()[0]
    
    # 25 items max
    new_index = min(last_index + 25, len(mlb_data))
    
    data_to_insert = mlb_data[last_index:new_index]
    cursor.executemany("""
        INSERT OR IGNORE INTO Locations (team_name, stadium_location)
        VALUES (?, ?)
        """, data_to_insert)
    
    cursor.execute("UPDATE Metadata SET last_inserted_index = ? WHERE id = 1", (new_index,))
    
    connection.commit()
    print(f"Inserted {len(data_to_insert)} rows into the database. New index is {new_index}.")
    connection.close()

if __name__ == "__main__":
    setup_database("mlb_data.db")

    mlb_data = scrape_mlb_data(url)

    if mlb_data:
        insert_into_database("mlb_data.db", mlb_data)
    else:
        print("No data to insert.")