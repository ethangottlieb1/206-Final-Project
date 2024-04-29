import requests
import sqlite3
import time
from datetime import datetime, timedelta

def setup_database():
    conn = sqlite3.connect('mlb_data.db')
    c = conn.cursor()
    c.execute('''
    CREATE TABLE IF NOT EXISTS Teams (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE
    )
    ''')
    c.execute('''
    CREATE TABLE IF NOT EXISTS Games (
        id INTEGER PRIMARY KEY,
        date TEXT,
        home_team_id INTEGER,
        away_team_id INTEGER,
        home_score INTEGER,
        away_score INTEGER,
        FOREIGN KEY (home_team_id) REFERENCES Teams (id),
        FOREIGN KEY (away_team_id) REFERENCES Teams (id),
        UNIQUE(date, home_team_id, away_team_id)  -- Unique constraint added
    )
    ''')
    c.execute('''
    CREATE TABLE IF NOT EXISTS LastFetched (
        id INTEGER PRIMARY KEY,
        last_date TEXT
    )
    ''')
    c.execute('INSERT OR IGNORE INTO LastFetched (id, last_date) VALUES (1, ?)', (datetime(2023, 9, 1).strftime('%Y-%m-%d'),))
    conn.commit()
    return conn

def get_last_fetched_date(conn):
    c = conn.cursor()
    c.execute('SELECT last_date FROM LastFetched WHERE id = 1')
    result = c.fetchone()
    return datetime.strptime(result[0], '%Y-%m-%d')

def update_last_fetched_date(conn, date):
    c = conn.cursor()
    c.execute('UPDATE LastFetched SET last_date = ? WHERE id = 1', (date.strftime('%Y-%m-%d'),))
    conn.commit()

# store data and limit calls
def fetch_and_store_data(conn, max_items=25):
    c = conn.cursor()
    last_date = get_last_fetched_date(conn)
    current_date = last_date
    end_date = datetime(2023, 9, 30)
    items_count = 0

# API 
    url_base = 'https://api.sportradar.com/mlb/trial/v7/en/games/2023/09/{day}/boxscore.json'
    api_key = 'Gv0Y6Bsxh2RncOy7fMS75jJTwC5ow8uu6Cd7fE20'

    while current_date <= end_date and items_count < max_items:
        day = current_date.strftime('%d')
        url = url_base.format(day=day)
        response = requests.get(url, params={'api_key': api_key})

        if response.status_code == 200:
            data = response.json()
            games = data.get('league', {}).get('games', [])
            for game in games:
                if items_count >= max_items:
                    break

                home_team = game['game']['home']['name']
                away_team = game['game']['away']['name']
                home_score = game['game']['home']['runs']
                away_score = game['game']['away']['runs']

                for team_name in [home_team, away_team]:
                    c.execute('INSERT OR IGNORE INTO Teams (name) VALUES (?)', (team_name,))

                c.execute('SELECT id FROM Teams WHERE name=?', (home_team,))
                home_team_id = c.fetchone()[0]
                c.execute('SELECT id FROM Teams WHERE name=?', (away_team,))
                away_team_id = c.fetchone()[0]

                try:
                    c.execute('''
                        INSERT INTO Games (date, home_team_id, away_team_id, home_score, away_score)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (current_date.strftime('%Y-%m-%d'), home_team_id, away_team_id, home_score, away_score))
                    items_count += 1
                except sqlite3.IntegrityError:
                    print(f"Duplicate game entry skipped for {current_date.strftime('%Y-%m-%d')}.")

            conn.commit()
            print(f"Data for {current_date.strftime('%Y-%m-%d')} stored successfully. Items stored: {items_count}")
        else:
            print(f"Failed to retrieve data for {current_date.strftime('%Y-%m-%d')}. Status code:", response.status_code)

        if items_count < max_items:
            current_date += timedelta(days=1)
            time.sleep(1)

    update_last_fetched_date(conn, current_date)

if __name__ == '__main__':
    conn = setup_database()
    fetch_and_store_data(conn)
    conn.close()