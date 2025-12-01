import requests 
import pandas as pd

def fetch_data(limit=50000):
    API_URL = "https://data.calgary.ca/resource/c2es-76ed.json"

    all_rows = []
    offset = 0

    while True:
        params = {
            "$limit": limit,
            "$offset": offset
        }

        r=requests.get(API_URL, params=params)
        data = r.json()

        if not data:
            break

        all_rows.extend(data)
        offset +=limit

    df = pd.DataFrame(all_rows)
    return df

if __name__ == "__main__":
      df = fetch_data()
      print(df.head())
      print(f"Total rows: {len(df)}")