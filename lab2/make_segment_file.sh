#!/usr/bin/env bash

CRAWL=CC-MAIN-2017-39
BASE_URL=https://commoncrawl.s3.amazonaws.com
LOCAL_SEGMENTS=$2
FILE_TYPE=wet

echo Creating $LOCAL_SEGMENTS segment file...

listing=crawl-data/$CRAWL/$FILE_TYPE.paths.gz

wget -O $FILE_TYPE.paths.gz --timestamping $BASE_URL/$listing

gzip -dc $FILE_TYPE.paths.gz | sed 's@^@s3://commoncrawl/@' > all_wet_$CRAWL.txt

head -$LOCAL_SEGMENTS all_wet_$CRAWL.txt > s3_segments.txt
if [ ! -f s3_segments.txt ]; then
    exit 1
else
	echo Created $LOCAL_SEGMENTS segment file.
	aws s3 cp s3_segments.txt s3://$1/input/s3_segments.txt
	echo Uploaded $LOCAL_SEGMENTS segment file.
fi
