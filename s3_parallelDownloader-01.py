#!/bin/env python3
'''
** Chaveat: not suitable for millions of files, it shows slow performance to get object list
ChangeLogs
- 2021.07.21: modified getObject function
  - used bucket.all instead of paginator
- 2021.07.20: first created
'''

#requirement
## python 3.3+
## boto3

import os
import boto3
from multiprocessing import Pool
from os import path, makedirs
from datetime import datetime

# variables
region = 'ap-northeast-2' ## change it with your region
bucket_name = 'your-own-bucket' ## change it with bucket name
prefix = 's3data01/data01' ## common prefix name of S3 object
input_list= [prefix+'dir01/', prefix+'dir02/'] ## sub_prefix name
local_dir = '/download_here/'  ## local directory to save files 
max_process = 256  ## number of concurrent processes 
debug_en = True
# end of variables ## you don't need to modify below codes.

# S3 session
s3 = boto3.resource('s3')
#s3_client = boto3.client('s3', region)
bucket = s3.Bucket(bucket_name)
######

# dividing folders by max concurrent processes
def divide_dirs_list(input_list, max_process):
    n = max(1, max_process)
    return (input_list[i:i+n] for i in range(0, len(input_list), n))

# download function
def downfiles(bucket_name, src_obj, dest_path):
    try:
        bucket.download_file(bucket_name, src_obj, dest_path)
        if debug_en:
            print("[dubug] downloading object: %s to %s" %(src_obj, dest_path))
    except:
        pass

def download_dir(bucket_name, sub_prefix):
    pool = Pool(max_process)
    mp_data = []
    for obj in bucket.objects.filter(Prefix=sub_prefix):
        src_obj = obj.key
        dest_path = local_dir + src_obj
        mp_data.append((bucket_name, src_obj, dest_path))
        os.path.dirname(dest_path) and os.makedirs(os.path.dirname(dest_path), exist_ok=True) 
    pool.starmap(downfiles, mp_data)
    return len(mp_data)

if __name__ == '__main__':
    print("starting script...")
    start_time = datetime.now()
    s3_dirs = input_list
    total_files = 0
    for s3_dir in s3_dirs:
        # multiprocessing tasks
        print("[Information] %s directory is downloading" % s3_dir)
        no_files = download_dir(bucket_name, s3_dir)
        total_files = total_files + no_files

    end_time = datetime.now()
    print('Duration: {}'.format(end_time - start_time))
    print('Total File numbers: %d' % total_files)
    print("ended")
