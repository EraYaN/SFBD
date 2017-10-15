#!/usr/bin/env python3

import argparse
import os
import re
import time

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from tempfile import NamedTemporaryFile

import boto3
import botocore

import logging

import phonenumberfilter as pnf


class PhoneNumbers:
    s3pattern = re.compile('^s3://([^/]+)/(.+)')

    output_schema = StructType([
        StructField("num", StringType(), True),
        StructField("urls", ArrayType(StringType()), True)
        ])


    def __init__(self, input_file, output_dir, name, partitions=None, local=False):
        self.name = name
        self.input_file = input_file
        self.output_dir = output_dir
        self.partitions = partitions
        self.local = local
        self.logger = None

    def run(self):
        if self.local:
            conf = SparkConf()
            conf = (conf
                    .set('spark.executor.memory', '16G')
                    .set('spark.driver.memory', '24G')
                    .set('spark.driver.maxResultSize', '8G')
                    .set("spark.executor.heartbeatInterval", "3600s"))
            sc = SparkContext(appName=self.name, conf=conf)
        else:
            sc = SparkContext(appName=self.name)
        
        #sc.setLogLevel('info')
        self.failed_segment = sc.accumulator(0)
        self.download_time = sc.accumulator(0.0)
        self.process_time = sc.accumulator(0.0)

        sqlc = SQLContext(sparkContext=sc)

        if self.partitions is None:
            self.partitions = sc.defaultParallelism

        
        self.log(sc,"Started...".format(self.partitions))       
        t0 = time.perf_counter()
        input_data = sc.textFile(self.input_file, minPartitions=self.partitions)
        segments = input_data.count()
        usedpartitions = input_data.getNumPartitions()
        self.log(sc,"Data has {} segments on {} partitions..".format(segments, usedpartitions))       
        phone_numbers = input_data.flatMap(self.process_warcs)        
        phone_numb_agg_web = phone_numbers.groupByKey().mapValues(list)
        
        sqlc.createDataFrame(phone_numb_agg_web, schema=self.output_schema) \
                .write \
                .mode('overwrite') \
                .format("parquet") \
                .save(self.output_dir)

        t1 = time.perf_counter()

        self.log(sc,"Found {} unique phone numbers in total.".format(phone_numb_agg_web.count()))
        self.log(sc,"New implementation took: {:.3f} seconds.".format(t1-t0))
        self.log(sc,"Download took: {0:.3f} seconds or {1:.3f} seconds per partition and {2:.3f} per segement.".format(self.download_time.value, self.download_time.value/usedpartitions, self.download_time.value/segments))
        self.log(sc,"Processing took: {0:.3f} seconds or {1:.3f} seconds per partition and {2:.3f} per segement.".format(self.process_time.value, self.process_time.value/usedpartitions, self.process_time.value/segments))
        self.log(sc,"Processed segments: {}".format(segments-self.failed_segment.value))
        self.log(sc,"Failed segments: {}".format(self.failed_segment.value))


    def log(self, sc, message, level="warn"):
        log = sc._jvm.org.apache.log4j.LogManager.getLogger(self.name)
        if level == "info":
            log.info(message)
        elif level == "warn":
            log.warn(message)
        else:
            log.warn("Level unknown for logging: {}".format(level))


    def process_warcs(self, input_uri):
        t_start = time.perf_counter()

        if input_uri.startswith('file:'):
            t_mid = t_start
            res = self.process_records(input_uri[5:], False)
            t_end = time.perf_counter()            

        elif input_uri.startswith('s3:/'):
            tempfileobj = self.process_s3_warc(input_uri)
            tempname = tempfileobj.name
            tempfileobj.close()
            
            res = self.process_records(tempname, True)
            t_end = time.perf_counter()            
        else:
            res = []

        self.download_time.add(t_mid - t_start)
        self.process_time.add(t_end - t_mid)
        return res


    def process_s3_warc(self, uri):
        try:
            no_sign_request = botocore.client.Config(signature_version=botocore.UNSIGNED)
            s3client = boto3.client('s3', config=no_sign_request)            
            s3match = self.s3pattern.match(uri)
            if s3match is None:
                print("ERROR: Invalid URI: {}".format(uri))
                self.failed_segment.add(1)
                return None
            bucketname = s3match.group(1)
            path = s3match.group(2)
            warctemp = NamedTemporaryFile(mode='w+b', delete=False)
            s3client.download_fileobj(bucketname, path, warctemp)

            return warctemp
        except BaseException as e:
            print("ERROR: Failed fetching {}\nError: {}".format(uri, e))
            self.failed_segment.add(1)

            return None


    def process_records(self, filename, is_s3=False):
        try:
            if is_s3:
                return pnf.load(filename, True, True)
            else:
                return pnf.load(filename, True, False)

        except BaseException as e:
            print("ERROR: Failed parsing with C implementation.\nError: {}".format(e))
            self.failed_segment.add(1)



if __name__ == "__main__":
    parser = argparse.ArgumentParser("Phone number analysis using Apache Spark")
    parser.add_argument("--input", '-i', metavar="segment_index",
                        type=str, required=True,
                        help="URI to input segment index")
    parser.add_argument("--output", '-o', metavar="output_dir",
                        type=str, required=True,
                        help="URI to output directory")
    parser.add_argument("--partitions", '-p', metavar="no_partitions",
                        type=int,
                        help="number of partitions in the input RDD")
    parser.add_argument("--name", '-n', metavar="application_name",
                        type=str, default="Phone Numbers",
                        help="override name of application")
    parser.add_argument("--local", '-l', action='store_true',
                        help="Run locally (set cluster mem sizes)")

    conf = parser.parse_args()
    pn = PhoneNumbers(conf.input, conf.output,
                      conf.name, partitions=conf.partitions, local=conf.local)
    pn.run()
