"""
This script is used to download comment data dumps from pushshift
"""
import requests
import os
import time

def download_file_streaming(url, location=""):
    local_filename = url.split('/')[-1]
    local_filename = os.path.join(location, local_filename)
    print(local_filename)
    # NOTE the stream=True parameter below
    print("starting download")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=819200000): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)
    print("download done")
    return local_filename


def download_file(url, location=""):
    local_filename = url.split('/')[-1]
    local_filename = os.path.join(location, local_filename)
    print(local_filename)
    print("starting download")
    r = requests.get(url)
    print("download done")
    r.raise_for_status()
    with open(local_filename, 'wb') as f:
            f.write(r.content)
    return local_filename

if __name__ == "__main__":
    start_time = time.monotonic()
    time_frames = ['2017-01', '2017-02', '2017-03', '2017-04', '2017-05', '2017-06', '2017-07', '2017-08', '2017-09', '2017-10', '2017-11']
    except_list = []
    for time_frame in time_frames:
        try:
            print(f"Starting time frame {time_frame}")
            download_file_streaming(f"https://files.pushshift.io/reddit/comments/RC_{time_frame}.bz2", "data/dumps/comments")
            print(f"Completed time frame {time_frame}")
        except Exception as e:
            print(f"Exception {e} for {time_frame}")
            except_list.append([time_frame])
    print(f"Excpetion list: {except_list}")
    print(f"Time taken: {time.monotonic() - start_time}")
