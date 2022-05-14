"""
This script is used to query data from pushshift bigquery tables
"""
from google.oauth2 import service_account
from dotenv import load_dotenv
from google.cloud import bigquery
import os
import csv
import time

load_dotenv("data_extr.env")
print(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

client = bigquery.Client()

start_time = time.monotonic()
sub_reddits = ('TheRedPill', 'MGTOW', 'KotakuInAction', 'MensRights', 'Braincels', 'marriedredpill', 'asktrp', 'askMRP', 'shortcels', 'IncelsWithoutHate', 'MGTOW2')
years = [2018, 2019]
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
for year in years:
    for month in months:
        print(f"Year {year} Month {month} started")
        with open(f"data/posts/Posts_{year}_{month}.csv", "w", newline="") as file_obj:
            csv_file_obj = csv.writer(file_obj)
            # sql = "SELECT * FROM `bigquery-public-data.usa_names.usa_1910_2013` WHERE state = 'TX' LIMIT 200"
            sql = f"""SELECT * FROM `fh-bigquery.reddit_posts.{year}_{month}` where subreddit IN  {sub_reddits}"""

            results = client.query(sql)
            i = 0
            for row in results:
                if i == 0:
                    csv_file_obj.writerow([key for key in row.keys()])
                    i  = 1
                csv_file_obj.writerow([val for val in row.values()])
        print(f"Year {year} Month {month} done")
print(f"Time taken: {time.monotonic() - start_time}")


