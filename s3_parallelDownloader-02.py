#!/bin/env python3

import os
import boto3
from multiprocessing import Pool, Process, Queue
from os import path, makedirs
from datetime import datetime

# variables
region = 'us-east-2' ## change it with your region
bucket_name = 'your-own-bucket'
prefix = 'data1/'
input_list= [prefix+'dir1/', prefix+'dir2/', prefix+'dir3/']
local_dir = '/download/here/'
max_process = 512
endpoint='s3.'+region+'awsamazon.com'
debug_en = False
# end of variables ## you don't need to modify below codes.
quit_flag = 'DONE'

# S3 session
#s3 = boto3.client('s3', region)
s3 = boto3.resource('s3',endpoint_url=endpoint, region_name=region)
bucket = s3.Bucket(bucket_name)

# dividing folders by max concurrent processes
def divide_dirs_list(input_list, max_process):
    n = max(1, max_process)
    return (input_list[i:i+n] for i in range(0, len(input_list), n))

# download function

def get_objs(sub_prefix, q):
    num_obj=1
    for obj in bucket.objects.filter(Prefix=sub_prefix):
        src_obj = obj.key
        dest_path = local_dir + src_obj
        #os.path.dirname(dest_path) and os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        mp_data = (src_obj, dest_path)
        #print('mpdata of getobj: ', mp_data)
        num_obj+=1
        q.put(mp_data)
    #print('all object list ingested')
    return num_obj
    #q.put(quit_flag)

def download_files(bucket, q):
    while True:
        mp_data = q.get()
        if mp_data == quit_flag:
            break
        src_obj = mp_data[0]
        dest_path = mp_data[1]
        os.path.dirname(dest_path) and os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        try:
            bucket.download_file(src_obj, dest_path)
            if debug_en:
                print("[dubug] downloading object: %s to %s" %(src_obj, dest_path))
        except:
            pass

def run_multip(max_process, download_files, q):
    p_list = []
    for i in range(max_process):
        p = Process(target = download_files, args=(bucket,q,))
        p_list.append(p)
        p.daemon = True
        p.start()
    return p_list

def finishq(q, p_list):
        for j in range(max_process):
            q.put(quit_flag)
        for pi in p_list:
            pi.join()

def download_dir(s3_dirs):
    q = Queue()
    total_obj = 0
    for s3_dir in s3_dirs:
        # multiprocessing tasks
        print("[Information] %s directory is downloading" % s3_dir)
        # run multiprocessing
        p_list = run_multip(max_process, download_files, q)
        # get object list and ingest to processes
        num_obj = get_objs(s3_dir, q)
        # sending quit_flag and join processes
        finishq(q, p_list)
        print("[Information] %s download is finished" % s3_dir)
        total_obj += num_obj
    return total_obj
if __name__ == '__main__':
    #print("starting script...")
    start_time = datetime.now()
    s3_dirs = input_list
    total_files = download_dir(s3_dirs)
    end_time = datetime.now()
    print('=============================================')
    #for d in down_dir:
    #    stored_dir = local_dir + d
    #    print("[Information] Download completed, data stored in %s" % stored_dir)
    print('Duration: {}'.format(end_time - start_time))
    print('Total File numbers: %d' % total_files)
    print('S3 Endpoint: %s' % endpoint)
    print("ended")
