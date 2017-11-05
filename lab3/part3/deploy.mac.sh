#!/bin/bash
echo "Assembling main JAR... (WSL)"
sbt package || exit 1
echo "Creating remote directory...."
ssh $1@kova-01.ewi.tudelft.nl 'mkdir -p ~/sfbd' || exit 1
ssh $1@kova-01.ewi.tudelft.nl 'mkdir -p ~/sfbd/output' || exit 1
echo "Deploying JARs...."
rsync -utRz --include="*.jar" --exclude="*" --progress ./target/scala-2.11/* $1@kova-01.ewi.tudelft.nl:~/sfbd || exit 1
echo "Running Application...."
ssh $1@kova-01.ewi.tudelft.nl 'cd ~/sfbd ;export SPARK_HOME=/data/spark/spark-2.0.1-bin-hadoop2.4; $SPARK_HOME/bin/spark-submit --class "VarDensity" --master local[*] --driver-memory 10g --executor-memory 3g ./target/scala-2.11/vardensity_2.11-1.0.jar 8 /data/spark/ref/dbsnp_138.hg19.vcf /data/spark/ref/ucsc.hg19.dict'
