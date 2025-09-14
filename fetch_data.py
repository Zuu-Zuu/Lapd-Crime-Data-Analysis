import requests
import os
import json
import time

# API and file config
BASE_URL = "https://data.lacity.org/resource/2nrs-mtv8.json"
LIMIT = 1000
MAX_ROWS = 2_000_000
YEAR_RANGE = (2020, 2024)
OUTPUT_FILE = "/home/zuuzuu/Desktop/lapd_project/data/raw/lapd_crime_2020_2024.json"
SLEEP = 0.5  # Prevent hitting rate limits

def fetch_all_data():
    all_data = []
    offset = 0

    while offset < MAX_ROWS:
        query = {
            "$limit": LIMIT,
            "$offset": offset,
            "$where": f"date_occ >= '{YEAR_RANGE[0]}-01-01T00:00:00' AND date_occ <= '{YEAR_RANGE[1]}-12-31T23:59:59'"
        }

        try:
            response = requests.get(BASE_URL, params=query)
            response.raise_for_status()
            data = response.json()

            if not data:
                break

            all_data.extend(data)
            offset += LIMIT
            time.sleep(SLEEP)

        except Exception as e:
            break

    with open(OUTPUT_FILE, "w") as f:
        json.dump(all_data, f)

    print(f"{OUTPUT_FILE} written successfully.")

if __name__ == "__main__":
    print(f"Fetching LAPD Crime Data ({YEAR_RANGE[0]}–{YEAR_RANGE[1]})...")
    fetch_all_data()
