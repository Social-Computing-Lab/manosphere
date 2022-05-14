"""
This script is used to extract data from the data dumps for specific users
"""


import time
import glob
import zreader
import json
import time
import re
import csv
import os
import xz
from bz2_reader import BZ2_LineReader
from multiprocessing import Pool

# field names that should be extracted for comments and submissions
field_names = {
"posts" : ['author', 'author_created_utc', 'author_fullname', 'created_utc', 'crosspost_parent', 'crosspost_parent_list', 'id', 'num_comments', 'score', 'selftext', 'subreddit', 'subreddit_id', 'subreddit_subscribers', 'subreddit_type', 'title', 'permalink'],
"comments" : ['author', 'author_created_utc', 'author_fullname', 'body', 'controversiality', 'created_utc', 'id', 'is_submitter', 'parent_id', 'subreddit', 'subreddit_id', 'subreddit_type', 'gilded', 'score', 'permalink', 'distinguished']
}

user_set = set()
author_regex = ""

# method to extract from .zst file
def extract_zst_to_csv(filename, rec_type, csv_path):
    print(f"started {os.path.basename(filename)}")
    try:
        start_time = time.monotonic()
        zrd = zreader.Zreader(filename, chunk_size=8192000)
        fields = field_names[rec_type]
        csv_writer = csv.DictWriter(open(csv_path, "a", newline=""), fieldnames=fields)
        # csv_writer.writeheader()
        count = 0
        missing_fields = set()
        for line in zrd.readlines():
            obj = json.loads(line)
            if "author" in obj:
                if obj["author"] in user_set:
                # if re.search(author_regex, obj['author'], re.IGNORECASE) is not None:
                    extr_obj = {}
                    for field in fields:
                        if field in obj:
                            extr_obj[field] = obj[field]
                        else:
                            missing_fields.add(field)
                            extr_obj[field] = ''
                    csv_writer.writerow(extr_obj)
                    count += 1
                    # if count % 1000 == 0:
                    #     print(f"{os.path.basename(filename)} {count} rows done")
    except Exception as e:
        print(f"{os.path.basename(filename)} {e}")
        return(f"{os.path.basename(filename)} {e}")
    print(f"{os.path.basename(filename)} Missing fields: {missing_fields}")
    print(f"{os.path.basename(filename)} Records count: {count}")
    print(f"{os.path.basename(filename)} Time taken: {time.monotonic() - start_time}")


# method to extract from .bz2 file
def extract_bz2_to_csv(filename, rec_type, csv_path):
    print(f"started {filename}")
    try:
        start_time = time.monotonic()
        bz2_rd = BZ2_LineReader(filename)
        fields = field_names[rec_type]
        csv_writer = csv.DictWriter(open(csv_path, "a", newline=""), fieldnames=fields)
        # csv_writer.writeheader()
        count = 0
        missing_fields = set()
        for line in bz2_rd.read_lines():
            obj = json.loads(line)
            if "author" in obj:
                if obj["author"] in user_set:
                # if re.search(author_regex, obj['author'], re.IGNORECASE) is not None:
                    extr_obj = {}
                    for field in fields:
                        if field in obj:
                            extr_obj[field] = obj[field]
                        else:
                            missing_fields.add(field)
                            extr_obj[field] = ''
                    csv_writer.writerow(extr_obj)
                    count += 1
                    # if count % 1000 == 0:
                    #     print(f"{count} rows done")
    except Exception as e:
        print(f"{filename} {e}")
        raise(e)
    print(f"{filename} Missing fields: {missing_fields}")
    print(f"{filename} Records count: {count}")
    print(f"{filename} Time taken: {time.monotonic() - start_time}")


#method to extract from .xz file
def extract_xz_to_csv(filename, rec_type, csv_path):
    print(f"started {filename}")
    try:
        start_time = time.monotonic()
        fields = field_names[rec_type]
        csv_writer = csv.DictWriter(open(csv_path, "a", newline=""), fieldnames=fields)
        # csv_writer.writeheader()
        count = 0
        missing_fields = set()
        xz_rd = xz.open(filename, mode="rt")
        for line in xz_rd:
            obj = json.loads(line)
            if "author" in obj:
                if obj["author"] in user_set:
                # if re.search(author_regex, obj['author'], re.IGNORECASE) is not None:
                    extr_obj = {}
                    for field in fields:
                        if field in obj:
                            extr_obj[field] = obj[field]
                        else:
                            missing_fields.add(field)
                            extr_obj[field] = ''
                    csv_writer.writerow(extr_obj)
                    count += 1
                    # if count % 1000 == 0:
                    #     print(f"{count} rows done")
    except Exception as e:
        print(f"{filename} {e}")
        raise(e)
    print(f"{filename} Missing fields: {missing_fields}")
    print(f"{filename} Records count: {count}")
    print(f"{filename} Time taken: {time.monotonic() - start_time}")

extractors = {"xz":extract_xz_to_csv, "bz2": extract_bz2_to_csv, "zst": extract_zst_to_csv}

# call the respective extractor method based on the compression type
def call_extractor(filename, rec_type, csv_path):
    ext = os.path.splitext(filename)[1][1:]
    extractor = extractors.get(ext)
    extractor(filename, rec_type, csv_path)


if __name__ == "__main__":
    start_time = time.monotonic()
    dumps_dir = "data/dumps"
    top_user_file = "data/primary/top_20_perc_users.csv"
    time_frames = {'2017-07', '2017-08', '2017-09', '2017-10', '2017-11', '2017-12', '2018-01', '2018-02', '2018-03', '2018-04', '2018-05', '2018-06', '2018-07', '2018-08', '2018-09', '2018-10', '2018-11', '2018-12', '2019-01', '2019-02', '2019-03', '2019-04', '2019-05', '2019-06'}
    output_dir = "data/primary/top_user_activity"
    record_type = "comments"
    user_range_start = 0
    user_range_end = 65335

    user_ranges = []

    for i in range((user_range_end - user_range_start)//2000):
        user_ranges.append((user_range_start+i*2000, user_range_start+(i+1)*2000))
    
    if user_ranges[-1][1] != user_range_end:
        user_ranges[-1] = (user_ranges[-1][0], user_range_end)
    
    print(user_ranges)

    top_users_csv = csv.reader(open(top_user_file, "r"))
    author_index = next(top_users_csv).index("author")

    for i in range(user_range_start):
        next(top_users_csv)
    
    
    dump_files = glob.glob(os.path.join(dumps_dir, record_type, "*.*"))
    dump_files = [dump_file for dump_file in dump_files if re.search(r"\d{4}\-\d{2}", dump_file).group() in time_frames]
    output_files = [os.path.join(output_dir, record_type, f"{os.path.splitext(os.path.basename(dump_file))[0]}.csv")
    for dump_file in dump_files]
    
    for user_range in user_ranges:
        user_set = set()
        start, end = user_range
        # read in the user ids of the users whose data we need to extract
        for i in range(end - start):
            user_set.add(next(top_users_csv)[author_index])
    

        # map the processes to different month's dumps
        pool = Pool(processes=16)
        t = pool.starmap(call_extractor, zip(dump_files, [record_type for i in range(len(dump_files))], output_files))

        entries = []
        for k in t:
            entries.append(k)
    
        print(entries)
        print(f"Range: {start} -> {end} done")
    print(f"Rows done: {user_range_start} -- {user_range_end}")
    print(f"Time taken: {time.monotonic() - start_time}")
    
