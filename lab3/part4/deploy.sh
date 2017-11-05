#!/bin/bash
echo "Assembling main JAR... (WSL)"
cmd.exe /C sbt package || exit 1
echo "Creating remote directory...."
ssh erdehaan@kova-01.ewi.tudelft.nl 'mkdir -p ~/sfbd' || exit 1
echo "Deploying config...."
rsync -utRz --progress config.xml erdehaan@kova-01.ewi.tudelft.nl:~/sfbd/config.xml || exit 1
echo "Deploying support files...."
rsync -utRz --progress ucsc.hg19.dict erdehaan@kova-01.ewi.tudelft.nl:~/sfbd/ucsc.hg19.dict || exit 1
rsync -utRz --progress compare/* erdehaan@kova-01.ewi.tudelft.nl:~/sfbd || exit 1
echo "Deploying JARs...."
rsync -utRz --include="*.jar" --exclude="*" --progress ./target/scala-2.11/* erdehaan@kova-01.ewi.tudelft.nl:~/sfbd || exit 1
rsync -utRz --include="*.jar" --exclude="*" --progress ./lib/* erdehaan@kova-01.ewi.tudelft.nl:~/sfbd || exit 1
echo "Running Application...."
ssh erdehaan@kova-01.ewi.tudelft.nl 'cd ~/sfbd ;export SPARK_HOME=/data/spark/spark-2.0.1-bin-hadoop2.4; $SPARK_HOME/bin/spark-submit --jars lib/htsjdk-1.143.jar --class "DNASeqAnalyzer" --master local[*] --driver-memory 32g target/scala-2.11/dnaseqanalyzer_2.11-1.0.jar'
