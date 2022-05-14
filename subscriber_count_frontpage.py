"""
This script is used to get the subscriber count of a subreddit from frontpagemetrics.com
"""
from operator import sub
import pandas as pd
import csv
from bs4 import BeautifulSoup
import requests
import time

# list of subreddits for which we need the count
subreddits = pd.read_csv("data/primary/top_user_activity/subreddit_na.csv")
subreddits = subreddits["subreddit"].values
print(len(subreddits))

#csv file where the count is updated
subscriber_count_csv = csv.writer(open("data/primary/top_user_activity/subscriber_count_n.csv", "a", newline=""))
subscriber_count_csv.writerow(["subreddit", "subscribers", "public_description", "not_available"])

for i, subreddit in enumerate(subreddits):
    url = f"https://frontpagemetrics.com/r/{subreddit}"
    page_content = requests.get(url)
    page_content.raise_for_status()
    if page_content.url == url:
        page_content = page_content.content
        html = BeautifulSoup(page_content, 'html.parser')
        subscriber_count = html.find("h2", attrs={"class": "rank"}).text
        public_desc = html.find("blockquote").text
        subscriber_count_csv.writerow([subreddit, subscriber_count, public_desc, False])
    else:
        subscriber_count_csv.writerow([subreddit, 0, "", True])
    if i % 1000 == 0:
        print(f"{i} rows done")
        time.sleep(5)
