#!/usr/bin/env python

import datetime
import os,sys,inspect
import argparse

from os import kill
from signal import alarm, signal, SIGALRM, SIGKILL
from subprocess import PIPE, Popen, call

import itertools
from multiprocessing import Pool, freeze_support
from operator import itemgetter
import fcntl
import errno
import time
import hashlib

import boto
import glob
import boto3

s3b = boto.s3.connect_to_region('eu-west-1')

# src : BUCKET_NAME/PREFIX
def get_files_old(src, debug):
  (bucket_name, prefix) = src.split('/', 1)
  s3 = boto.s3.connect_to_region('eu-west-1')

  if debug > 1000:
    print '- connected to S3 :', s3
    print '- read bucket : ', bucket_name
    
  if s3:
    bucket = s3.get_bucket(bucket_name)
    
    if debug > 1000:
      print '- connected to AWS : ', bucket
      print '- reading : ', bucket_name  
    return bucket.list(prefix=prefix)
  
  return


def copy_func(i, dest, debug=0):
  pid = os.getpid()

  path = dest.split('/',1)
  if debug > 1000:
    print path
      
  spath = i.key.split('/')
  if debug > 500:
    print spath
    
  dpath = "%s/%s" % (path[1], spath[-1])
  
  if debug > 100:
    print '%d %s %d ( %s) ' % (pid, dpath, i.size, i.md5)

  i.copy(path[0], dpath)
  return

def md5_func(i, dest, debug=0):
  md5 = i.md5
  if md5 is None:
    try:
      m = hashlib.md5()
      m.update(i.get_contents_as_string())
      md5 = m.hexdigest()
    except Exception, e:
      md5 = "Exception-%s" % (e)
      
  if debug > 100:
    print '%s %s' % (md5, i.key)

  if md5 is not None:
    pid = os.getpid()
    fname = "%d-%s" % (pid, dest)
    
    spath = i.key.split('/')
    with open(fname, 'a') as fh:
      fh.write("%s %s\n" % (md5, spath[-1]))
      
  return

def envoke_copy(params):
  return copy_func(*params)
def envoke_md5(params):
  return md5_func(*params)


def merge_func(i, bucket, dest, debug=0):
    if i['Size'] > 0: 
        bucket_obj = s3b.get_bucket(bucket)
        obj = bucket_obj.get_key(i['Key'])

        pid = os.getpid()
        fname = "%d-%s" % (pid, dest)
        with open(fname, 'a') as fh:
            obj.get_contents_to_file(fh)
            fh.write("\n")
    return

def envoke_merge(params):
    return merge_func(*params)

# src : BUCKET_NAME/PREFIX
def get_files(bucket, prefix, suffix, debug):
    s3 = boto3.client('s3') 
    paginator = s3.get_paginator('list_objects')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    return page_iterator.search("Contents[?ends_with(Key, '"+suffix+"')][]")
    
def get_arguments():
    parser = argparse.ArgumentParser(
        description = "S3 Utils",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('cmd', help='command to run [ merge|md5|cp ]')
    parser.add_argument('src', help='source folder, e.g my-bucket/my-prefix1')
    parser.add_argument('--sfx', dest='suffix', help='suffix, e.g .csv', default='')
    
    parser.add_argument('--l', dest='log', help='log file', default='/tmp/s3utils.log')
    parser.add_argument('--o', dest='dst', help='destination file or folder', default="output")
    parser.add_argument('--num', dest='num', help='number of files to copy', type=int, default=0)
    parser.add_argument('--p', type=int, dest='procnum', help='how many processes to start in the pool', default=1)
    parser.add_argument('--v', type=int, dest='debug', help='verbosity level', default = 0)
    return parser.parse_args()

def main():
    if len(sys.argv) < 3:
        sys.argv.append("-h")
    args = get_arguments()

    started = datetime.datetime.now()
    if args.debug > 100:
        print "- started at %s" % started
    
    (bucket, prefix) = args.src.split('/', 1)
  # sometime we only need a sample from a folder, --num NUM will take just NUM objects from a folder
    full_list = get_files(bucket, prefix, args.suffix, args.debug)
    list_to_process = full_list

    if args.num > 0:
        list_to_process = itertools.islice(full_list,args.num)
  
  # run it in parallel to make it faster
    pool = Pool(processes=args.procnum) 
  
    if args.cmd == 'copy':
        pool.map(envoke_copy, itertools.izip(list_to_process, itertools.repeat(args.dst), itertools.repeat(args.debug)))
    elif args.cmd == 'md5':
        pool.map(envoke_md5, itertools.izip(list_to_process, itertools.repeat(args.dst), itertools.repeat(args.debug)))
    elif args.cmd == 'merge':  
        pool.map(envoke_merge, itertools.izip(list_to_process, itertools.repeat(bucket), itertools.repeat(args.dst), itertools.repeat(args.debug)))
    else:
        print 'Unknown command: %s' % (args.cmd)
  
    pool.close()
    pool.join()
  
    if args.cmd in ['md5','merge']:
    # cat *-{args.dst} >> {args.dst}
    # rm *-{args.dst}

        pattern = "*-%s" % args.dst
        tmp_files = glob.glob(pattern)
        with open(args.dst, "wb") as outfile:
            for f in tmp_files:
                with open(f, "rb") as infile:
                    outfile.write(infile.read())
                os.remove(f)
  
    finished = datetime.datetime.now()
    if args.debug > 100:
        print '- took %s s' % (finished - started)

if __name__ == "__main__":
    freeze_support()
    main()