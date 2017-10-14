#!/usr/bin/env bash

PIP3_INSTALL="sudo pip-3.4 install"
python_dependencies="warcio requests requests_file boto3 botocore py4j"

for dep in $python_dependencies; do
    $PIP3_INSTALL $dep
done;

aws s3 cp s3://et4310-sbd-3/phonenumberfiltermodule/setup.py ~/
aws s3 cp s3://et4310-sbd-3/phonenumberfiltermodule/prefixes.h ~/
aws s3 cp s3://et4310-sbd-3/phonenumberfiltermodule/phonenumberfiltermodule.cpp ~/
aws s3 cp --recursive s3://et4310-sbd-3/gzstream ~/gzstream

(cd ~ && sudo python3 setup.py install)
