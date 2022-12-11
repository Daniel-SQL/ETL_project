# Import needed libraries

from serpapi import GoogleSearch
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import sqlite3
import os

# Let's get the environment variables. USER_ID and DB_PASS are used to connect to the PostgreSQL database.

API_KEY = os.environ['API_KEY']
USER_ID = os.environ['USER_ID']
DB_PASS = os.environ['DB_PASS']

# We need certain parameters in order to use the SerpApi.

params = {
        "api_key": f"{API_KEY}",
        "device": "desktop",
        "engine": "google_jobs",
        "google_domain": "google.com",
        "start": 0,
        "q": "data engineer california",
        "chips": "date_posted:today",
        "gl": "us",
        "hl": "en"
         }

search = GoogleSearch(params)
jobs = []
page_num = 0

# Extract data from SerpApi using their API.

while True:

    results = search.get_dict()

    if "error" in results:
        print(results["error"])
        break

    page_num += 1
    print(f"Current page: {page_num}")


    for result in results["jobs_results"]:
        jobs.append({
            "title": result["title"],
            "company_name": result["company_name"],
            "location": result["location"],
            "description": result["description"]
        })

    params["start"] += 10

# Let's create a Pandas DataFrame

data = pd.DataFrame(jobs)

# Let's load the data in our Sqlite database.

conn = None

try:
    conn = sqlite3.connect('C:\\Users\\Daniel\\Desktop\\d.db')
    data.to_sql('california_data_jobs', conn, index = False, if_exists = 'append')
    print('We successfully loaded ' + str(len(data)) + ' rows of data into our table in our Sqlite database !')

except Exception as error:
    print(error)

finally:
    if conn is not None:
        conn.close()

#Let's also load the data in our PostgreSQL database.

engine = None


try:
    engine = create_engine(f'postgresql+psycopg2://{USER_ID}:{DB_PASS}@localhost/projectdb')
    data.to_sql('california_data_jobs', engine, index = False, if_exists = 'append')
    print('We successfully loaded ' + str(len(data)) + ' rows of data into our table in our PostgreSQL database !')

except Exception as error:
    print(error)

finally:
    if engine is not None:
        engine.dispose()



