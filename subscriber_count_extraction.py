"""
This script is used to get the subscriber count using thr reddit api. Need a reddit developer account to access this API
"""
from time import time
from xxlimited import new
import pandas as pd
import csv
import requests
import time

# list of subreddits for which we need the subscriber count
subreddits = pd.read_csv("data/primary/top_user_activity/subreddit_wise_activity.csv")
subreddits = subreddits["subreddit"].values
subreddits = subreddits[73739:]

# csv file where subscriber count will be updated
subscriber_count_csv = csv.writer(open("data/primary/top_user_activity/subscriber_count.csv", "a", newline=""))
# headers needed for reddit api request
headers = {'User-Agent': 'api_script/0.0.1'}
TOKEN = "Insert the token generated using your credentials"
headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}
subscriber_count_csv.writerow(["subreddit", "subscribers", "public_description", "is_banned", "not_available"])

# enumerate through the list of subreddits and get the subscriber count
for i, subreddit in enumerate(subreddits):
    url = f"https://oauth.reddit.com/r/{subreddit}/about"
    about_json = requests.get(url, headers=headers)
    about_json = about_json.json()
    about_json = about_json.get("data", about_json)
    if "subscribers" in about_json:
        subscriber_count_csv.writerow([subreddit, about_json.get("subscribers"), about_json.get("public_description"), False, False])
    else:
        subscriber_count_csv.writerow([subreddit, about_json.get("subscribers", 0), about_json.get("public_description", ""), about_json.get("reason") in ["banned", "quarantined"], True])
    if i % 1000 == 0:
        print(f"{i} rows done")
        time.sleep(5)