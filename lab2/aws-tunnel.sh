#!/usr/bin/env bash

CLUSTER=$1

trap '{ echo "Got Ctrl-C.  Aborting." ; exit 1; }' INT
if [ -z "$1" ]
  then
    CLUSTER=`aws emr list-clusters --active --output text | awk -F"\t" '$1=="CLUSTERS" {print $2}'`
fi
if [ -z "$CLUSTER" ]
  then
	echo No cluster given or active..
	exit 1
  else
	echo Opening tunnel to $CLUSTER..

	chmod 600 ~/SparkCluster.pem
	aws emr socks --cluster-id $CLUSTER --key-pair-file ~/SparkCluster.pem
fi
