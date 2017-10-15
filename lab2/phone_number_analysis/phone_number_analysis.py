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

import phonenumberfilter as pnf


class PhoneNumbers:

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

        self.failed_record_parse = sc.accumulator(0)
        self.failed_segment = sc.accumulator(0)

        sqlc = SQLContext(sparkContext=sc)

        if self.partitions is None:
            self.partitions = sc.defaultParallelism

        self.log(sc, "Started..")
        t0 = time.time()
        input_data = sc.textFile(self.input_file, minPartitions=self.partitions)

        phone_numbers = input_data.flatMap(self.process_warcs).flatMap(lambda x: x)

        phone_numb_agg_web = phone_numbers.groupByKey().mapValues(list)

        sqlc.createDataFrame(phone_numb_agg_web, schema=self.output_schema) \
                .write \
                .mode('overwrite') \
                .format("parquet") \
                .save(self.output_dir)

        t1 = time.time()
        self.log(sc, "Found {} unique phone numbers in total.".format(phone_numb_agg_web.count()))
        self.log(sc, "New implementation took: {:.3f} seconds.".format(t1-t0))
        t2 = time.time()

        self.log(sc, "Failed segments: {}".format(self.failed_segment.value))
        self.log(sc, "Failed parses: {}".format(self.failed_record_parse.value))


    def log(self, sc, message, level="warn"):
        log = sc._jvm.org.apache.log4j.LogManager.getLogger(self.name)
        if level == "info":
            log.info(message)
        elif level == "warn":
            log.warn(message)
        else:
            log.warn("Level unknown for logging: {}".format(level))


    def process_warcs(self, input_uri):
        t_start = time.time()

        if input_uri.startswith('file:'):
            res = self.process_records(input_uri[5:], False)
            t_end = time.time()
            print("##Download: {0};{1:.3f}".format(input_uri[5:], 0))
            print("##Process: {0};{1:.3f}".format(input_uri[5:], t_end - t_start))

        elif input_uri.startswith('s3:/'):
            tempfileobj = self.process_s3_warc(input_uri)
            tempname = tempfileobj.name
            tempfileobj.close()
            print("Passing {} temp file to the C code.".format(tempname))

            t_mid = time.time()
            res = self.process_records(tempname, True)
            t_end = time.time()

            print("##Download: {0};{1:.3f}".format(input_uri[5:], t_mid - t_start))
            print("##Process: {0};{1:.3f}".format(input_uri[5:], t_end - t_mid))
        else:
            res = []

        return res


    def process_s3_warc(self, uri):
        try:
            no_sign_request = botocore.client.Config(signature_version=botocore.UNSIGNED)
            s3client = boto3.client('s3', config=no_sign_request)
            s3pattern = re.compile('^s3://([^/]+)/(.+)')
            s3match = s3pattern.match(uri)
            if s3match is None:
                # self.log("Invalid URI: {}".format(uri), level='error')
                self.failed_segment.add(1)
                return None
            bucketname = s3match.group(1)
            path = s3match.group(2)
            warctemp = NamedTemporaryFile(mode='w+b', delete=False)
            s3client.download_fileobj(bucketname, path, warctemp)

            return warctemp
        except BaseException as e:
            # self.log("Failed fetching {}\nError: {}".format(uri, e), level='error')
            self.failed_segment.add(1)

            return None


    def process_records(self, filename, is_s3=False):
        try:
            if is_s3:
                yield pnf.load(filename, True, True)
            else:
                yield pnf.load(filename, True, False)

        except BaseException as e:
            # self.log("Failed parsing with C implementation.\nError: {}".format(e), level='error')
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
