"""
This script is used to read in a list of bot accounts and delete the posts made by them from the dataset.
"""

import csv
import time
import os
import pandas as pd

bots_csv = csv.reader(open("data/dumps/bots_in_data.csv", "r"))
next(bots_csv)
bots_list = set()
for row in bots_csv:
    bots_list.add(row[0])

bots_list.add("[deleted]")
bots_list.add("AutoModerator")
bots_list = {'RSDbot', 'AliaArchiverBot', 'video_descriptionbot', 'Yasuo_Spelling_Bot', 'mailmygovNNBot', 'dejavubot', 'mimesisBot', 'Sub_Corrector_Bot', 'Subjunctive__Bot', 'video_descriptbotbot', 'bot_replying_bot'}
timeframes = ['2017-07', '2017-08', '2017-09', '2017-10', '2017-11', '2017-12', '2018-01', '2018-02', '2018-03', '2018-04', '2018-05', '2018-06', '2018-07', '2018-08', '2018-09', '2018-10', '2018-11', '2018-12', '2019-01', '2019-02', '2019-03', '2019-04', '2019-05', '2019-06']
data_dir = "data/primary/cleaned"
output_dir = "data/primary/cleaned/"
rec_types = ["posts", "comments"]
prefixes = {"posts": "RS", "comments": "RC"}

for rec_type in rec_types:
    for timeframe in timeframes:
        print(os.path.join(data_dir, rec_type, f"{prefixes[rec_type]}_{timeframe}.csv"))
        posts_df = pd.read_csv(os.path.join(data_dir, rec_type, f"{prefixes[rec_type]}_{timeframe}.csv"))
        print(len(posts_df))
        filtered_df = posts_df[~posts_df["author"].isin(bots_list)]
        filtered_df.to_csv(os.path.join(output_dir, rec_type, f"{prefixes[rec_type]}_{timeframe}.csv"))
        print(len(filtered_df))
        del posts_df
        del filtered_df
        


