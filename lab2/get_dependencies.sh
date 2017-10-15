#!/usr/bin/env bash

PIP3_INSTALL="sudo pip-3.4 install"
python_dependencies="requests requests_files boto3 botocore py4j"

for dep in $python_dependencies; do
    $PIP3_INSTALL $dep
done;

aws s3 cp s3://$1/phonenumberfiltermodule/setup.py ~/
aws s3 cp s3://$1/phonenumberfiltermodule/prefixes.h ~/
aws s3 cp s3://$1/phonenumberfiltermodule/phonenumberfiltermodule.cpp ~/
aws s3 cp --recursive s3://$1/gzstream ~/gzstream

(cd ~ && sudo python3 setup.py install)

CRAWL=CC-MAIN-2017-39
BASE_URL=https://commoncrawl.s3.amazonaws.com
LOCAL_SEGMENTS=$2
FILE_TYPE=wet

echo Creating $LOCAL_SEGMENTS segment file...

listing=crawl-data/$CRAWL/$FILE_TYPE.paths.gz

mkdir -p input/

wget --timestamping $BASE_URL/$listing

gzip -dc $FILE_TYPE.paths.gz | sed 's@^@s3://commoncrawl/@' | head -$LOCAL_SEGMENTS > input/s3_segments.txt

echo Created $LOCAL_SEGMENTS segment file.
