"""
This script is used to fetch data using pmaw package
"""

import pmaw
import pandas as pd
import datetime
import time

from pmaw import PushshiftAPI

api = PushshiftAPI(num_workers=20, max_sleep=30)


def get_epochs_for_month(year: int, month: int):
    return (int(datetime.datetime(year, month, 1).timestamp()), int(datetime.datetime(year, month+1, 2).timestamp())) if month < 12 else (int(datetime.datetime(year, month, 1).timestamp()), int(datetime.datetime(year+1, 1, 1).timestamp()))


def get_reddit_data_pmaw(start: int, end: int, subreddit: str, type: str):
    
    if type == "comments":
        results = api.search_comments(after=start, before=end, subreddit=subreddit)
    else:
        results = api.search_submissions(after=start, before=end, subreddit=subreddit)
    return [result for result in results]



subreddits = ['PhilosophyOfRape', 'PussyPass', 'MensRights', 'seduction', 'pickup', 'askseddit', 'fPUA', 'pua', 'rsd', 'puascience', 'Incels', 'Braincels', 'TheRedPill', 'marriedredpill', 'RedPillWives', 'RedPillWomen', 'askTRP', 'RedPillParenting', 'thankTRP', 'becomeaman', 'altTRP', 'GEOTRP', 'TRPOffTopic', 'askanincel', 'BlackPillScience', 'IncelsWithoutHate', 'ForeverAlone', 'MensRightsLinks', 'MensRightsLaw', 'LadyMRAs', 'MRRef', 'MRActivism', 'masculism', 'Intactivists', 'Egalitarianism', 'Gold_Digger', 'MgtowMusic', 'MgtowBooks', 'MGTOW', 'Malecels', 'MaleForeverAlone', '1ncels', 'IncelsPurgatory', 'Truecels', 'IncelBrotherhood', 'LonelyNonViolentMen', 'Foreveraloneteens', 'ForeverAloneLondon', 'gaycel', 'incelselfies', 'gymcels', 'ForeverUnwanted', 'IncelDense', 'SupportCel', 'ForeverAloneDating', 'Truefemcels']
# "PhilosophyOfRape,PussyPass,MensRights,seduction,pickup,askseddit,fPUA,pua,rsd,puascience,Incels,Braincels,TheRedPill,marriedredpill,RedPillWives,RedPillWomen,askTRP,RedPillParenting,thankTRP,becomeaman,altTRP,GEOTRP,TRPOffTopic,askanincel,BlackPillScience,IncelsWithoutHate,ForeverAlone,MensRightsLinks,MensRightsLaw,LadyMRAs,MRRef,MRActivism,masculism,Intactivists,Egalitarianism,Gold_Digger,MgtowMusic,MgtowBooks,MGTOW,Malecels,MaleForeverAlone,1ncels,IncelsPurgatory,Truecels,IncelBrotherhood,LonelyNonViolentMen,Foreveraloneteens,ForeverAloneLondon,gaycel,incelselfies,gymcels,ForeverUnwanted,IncelDense,SupportCel,ForeverAloneDating,Truefemcels"
years = [2021]
# months = [1]
months = [9]
except_list = []
for year in years:
    for month in months:
        try:
            start_time = time.monotonic()
            print(f"{year} and {month} started at {start_time}")
            start_epoch, end_epoch = get_epochs_for_month(year, month)
            data = get_reddit_data_pmaw(start_epoch, end_epoch, ",".join(subreddits), "posts")
            print(f"{len(data)} records retrieved")
            data_df = pd.DataFrame(data)
            data_df.to_csv(f"data/posts/new/posts_{year}_{month}.csv")
            print(f"{year} and {month} completed. Time taken: {time.monotonic() - start_time}")
        except Exception as e:
            print(f"Exception while getting data for {month} month. {e}")
            except_list.append(month)
print(except_list)
print("Process done")

