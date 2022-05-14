"""
This script is used to extract data from the reddit data dumps for specific subreddits.
"""
from statistics import mode
import time
from multiprocessing import Pool
import glob
import zreader
import ujson as json
import time
import re
import csv
import os
from bz2_reader import BZ2_LineReader
from xz_reader import XZ_LineReader
import xz

# fields to be extracted for submissions and comments
field_names = {
"posts" : ['author', 'author_created_utc', 'author_fullname', 'created_utc', 'crosspost_parent', 'crosspost_parent_list', 'id', 'num_comments', 'score', 'selftext', 'subreddit', 'subreddit_id', 'subreddit_subscribers', 'subreddit_type', 'title', 'permalink'],
"comments" : ['author', 'author_created_utc', 'author_fullname', 'body', 'controversiality', 'created_utc', 'id', 'is_submitter', 'parent_id', 'subreddit', 'subreddit_id', 'subreddit_type', 'gilded', 'score', 'permalink', 'distinguished']
}

# create a regex for subreddits
subs = ['PhilosophyOfRape', 'PussyPass', 'MensRights', 'seduction', 'pickup', 'askseddit', 'fPUA', 'pua', 'rsd', 'puascience', 'Incels', 'Braincels', 'TheRedPill', 'marriedredpill', 'RedPillWives', 'RedPillWomen', 'askTRP', 'RedPillParenting', 'thankTRP', 'becomeaman', 'altTRP', 'GEOTRP', 'TRPOffTopic', 'askanincel', 'BlackPillScience', 'IncelsWithoutHate', 'ForeverAlone', 'MensRightsLinks', 'MensRightsLaw', 'LadyMRAs', 'MRRef', 'MRActivism', 'masculism', 'Intactivists', 'Egalitarianism', 'Gold_Digger', 'MgtowMusic', 'MgtowBooks', 'MGTOW', 'Malecels', 'MaleForeverAlone', '1ncels', 'IncelsPurgatory', 'Truecels', 'IncelBrotherhood', 'LonelyNonViolentMen', 'Foreveraloneteens', 'ForeverAloneLondon', 'gaycel', 'incelselfies', 'gymcels', 'ForeverUnwanted', 'IncelDense', 'SupportCel', 'ForeverAloneDating', 'Truefemcels']
subs = "|".join([f"({sub})" for sub in subs])
subs = f"^({subs})$"

decompressors = {"bz2":BZ2_LineReader, "xz":XZ_LineReader}

# zst compression extractor
def extract_zst_to_csv(filename, rec_type):
    print(f"started {os.path.basename(filename)}")
    try:
        start_time = time.monotonic()
        zrd = zreader.Zreader(filename, chunk_size=8192000)
        csv_path = os.path.join(os.path.dirname(filename), "csv", f"{os.path.splitext(os.path.basename(filename))[0]}.csv")
        fields = field_names[rec_type]
        csv_writer = csv.DictWriter(open(csv_path, "w", newline=""), fieldnames=fields)
        csv_writer.writeheader()
        count = 0
        missing_fields = set()
        for line in zrd.readlines():
            obj = json.loads(line)
            if "subreddit" in obj:
                # subreddit regex to match only specific subreddits
                if re.search(subs, obj['subreddit'], re.IGNORECASE) is not None:
                    extr_obj = {}
                    for field in fields:
                        if field in obj:
                            extr_obj[field] = obj[field]
                        else:
                            missing_fields.add(field)
                            extr_obj[field] = ''
                    csv_writer.writerow(extr_obj)
                    count += 1
                    if count % 1000 == 0:
                        print(f"{os.path.basename(filename)} {count} rows done")
    except Exception as e:
        print(f"{os.path.basename(filename)} {e}")
        return(f"{os.path.basename(filename)} {e}")
    print(f"{os.path.basename(filename)} Missing fields: {missing_fields}")
    print(f"{os.path.basename(filename)} Records count: {count}")
    print(f"{os.path.basename(filename)} Time taken: {time.monotonic() - start_time}")


# bz2 compression
def extract_bz2_to_csv(filename, rec_type):
    print(f"started {filename}")
    try:
        start_time = time.monotonic()
        bz2_rd = BZ2_LineReader(filename)
        csv_path = os.path.join(os.path.dirname(filename), "csv", f"{os.path.splitext(os.path.basename(filename))[0]}.csv")
        fields = field_names[rec_type]
        csv_writer = csv.DictWriter(open(csv_path, "w", newline=""), fieldnames=fields)
        csv_writer.writeheader()
        count = 0
        missing_fields = set()
        for line in bz2_rd.read_lines():
            obj = json.loads(line)
            if "subreddit" in obj:
                if re.search(subs, obj['subreddit'], re.IGNORECASE) is not None:
                    extr_obj = {}
                    for field in fields:
                        if field in obj:
                            extr_obj[field] = obj[field]
                        else:
                            missing_fields.add(field)
                            extr_obj[field] = ''
                    csv_writer.writerow(extr_obj)
                    count += 1
                    if count % 1000 == 0:
                        print(f"{count} rows done")
    except Exception as e:
        print(f"{filename} {e}")
        raise(e)
    print(f"{filename} Missing fields: {missing_fields}")
    print(f"{filename} Records count: {count}")
    print(f"{filename} Time taken: {time.monotonic() - start_time}")

# xz compression
def extract_xz_to_csv(filename, rec_type):
    print(f"started {filename}")
    try:
        start_time = time.monotonic()
        csv_path = os.path.join(os.path.dirname(filename), "csv", f"{os.path.splitext(os.path.basename(filename))[0]}.csv")
        fields = field_names[rec_type]
        csv_writer = csv.DictWriter(open(csv_path, "w", newline=""), fieldnames=fields)
        csv_writer.writeheader()
        count = 0
        missing_fields = set()
        xz_rd = xz.open(filename, mode="rt")
        for line in xz_rd:
            obj = json.loads(line)
            if "subreddit" in obj:
                if re.search(subs, obj['subreddit'], re.IGNORECASE) is not None:
                    extr_obj = {}
                    for field in fields:
                        if field in obj:
                            extr_obj[field] = obj[field]
                        else:
                            missing_fields.add(field)
                            extr_obj[field] = ''
                    csv_writer.writerow(extr_obj)
                    count += 1
                    if count % 1000 == 0:
                        print(f"{count} rows done")
    except Exception as e:
        print(f"{filename} {e}")
        raise(e)
    print(f"{filename} Missing fields: {missing_fields}")
    print(f"{filename} Records count: {count}")
    print(f"{filename} Time taken: {time.monotonic() - start_time}")


if __name__ == '__main__':
    INPUT_FILES_FOLDER = "data/dumps"
    record_types = ["comments"]
    compression = "bz2"

    process_start_time = time.monotonic()
    for record_type in record_types:
        time_frames = ['2017-07', '2017-08', '2017-09', '2017-10', '2017-11']
        # get paths of dumps from which we need to extract the data
        file_names = [os.path.join(INPUT_FILES_FOLDER, record_type, f"RC_{time_frame}.{compression}") for time_frame in time_frames]
        print(file_names)
        
        indices = range(len(file_names))
        print(file_names)
        # map a process to each dump
        pool = Pool(processes=16)              
        t = pool.starmap(extract_bz2_to_csv, zip(file_names, [record_type for i in range(len(file_names))]))

        entries = []
        for k in t:
            entries.append(k)

        print(entries)
    print(f"Total process time: {time.monotonic() - process_start_time}")
