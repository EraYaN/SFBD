#!/usr/bin/env bash

PIP3_INSTALL="sudo pip-3.4 install"
python_dependencies="requests requests_files boto3 botocore py4j warcio"

for dep in $python_dependencies; do
    $PIP3_INSTALL $dep
done;

aws s3 cp s3://$1/phonenumberfiltermodule/setup.py ~/
aws s3 cp s3://$1/phonenumberfiltermodule/prefixes.h ~/
aws s3 cp s3://$1/phonenumberfiltermodule/phonenumberfiltermodule.cpp ~/
aws s3 cp s3://$1/phonenumberfiltermodule/timing.h ~/
aws s3 cp s3://$1/phonenumberfiltermodule/timing.cpp ~/
aws s3 cp --recursive s3://$1/gzstream ~/gzstream

(cd ~ && sudo python3 setup.py install)
