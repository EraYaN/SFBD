#!/usr/bin/env bash

CLUSTER=$1

trap '{ echo "Got Ctrl-C.  Aborting." ; exit 1; }' INT
if [ -z "$1" ]
  then
    CLUSTER=`aws emr list-clusters --max-items 1 --output text | awk -F"\t" '$1=="CLUSTERS" {print $2}'`
fi
if [ -z "$CLUSTER" ]
  then
	echo No cluster given and none exist..
	exit 1
else
	echo Grabbing logs for $CLUSTER...

	aws s3 cp --recursive --exclude "*" --include "application_*_0001/container_*_01_000001/stderr.gz" s3://scfbd-emr/logs/$CLUSTER/containers ./logs/$CLUSTER/
	echo Decompressing logs for $CLUSTER...
	names=`find ./logs/$CLUSTER/ -name stderr.gz`
	
	for name in $names; do
			gzip -fd $name
	done;
	echo Processing logs for $CLUSTER...
	uncompressed_names=`find ./logs/$CLUSTER/ -name stderr`

	for uncompressed_name in $uncompressed_names; do
			grep "PhoneNumberFilteringApp:" $uncompressed_name
	done;
fi
