# s3utils
AWS S3 utility commands that can be run in parallel to take advantage of the multi-core processors

usage: s3utils.py cmd src [-h] [--l LOG] [--o DST] [--num NUM] [--p PROCNUM] [--v DEBUG]
                 
positional arguments:
  cmd          command to run [ merge | md5 | cp ]
  src          source folder, e.g my-bucket/my-prefix1

optional arguments:
  -h, --help   show this help message and exit
  --l LOG      log file (default: /tmp/s3utils.log)
  --o DST      destination file or folder (default: output)
  --num NUM    number of files to copy (default: 0 ( all ) )
  --p PROCNUM  how many processes to start in the pool (default: 1)
  --v DEBUG    verbosity level (default: 0)

*** merge ***

 bash$ bin/s3utils.py merge mybucket/folder_with_csv --o allcsv.csv --p 10 

will download all files from s3://mybucket/folder_with_csv and merge them together into allcsv.csv running 10 downloads in parallel


*** md5 ***

 bash$ bin/s3utils.py md5 mybucket/myfolder --o myfolder.md5 --p 10 

will get MD5 checksum for all files in s3://mybucket/myfolder and store them in myfolder.md5 running 10 operations in parallel

*** copy ***

 bash$ bin/s3utils.py copy mybucket/myfolder --o newbucket/newfolder --p 10 

will copy all files from s3://mybucket/myfolder to s3://newbucket/newfolder copying 10 files in parallel
** TODO: for cross account copy add --acl bucket-owner-full-control


*** --num ****

sometimes you only need to get just few files from a folder
--num X will make sure only X random files from the source folder are merged|copied|md5ied 
