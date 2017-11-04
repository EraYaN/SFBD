#!/bin/bash
echo "Assembling main JAR... (WSL)"
cmd.exe /C sbt assembly || exit 1
echo "Creating remote directory...."
ssh erdehaan@kova-01.ewi.tudelft.nl 'mkdir -p ~/sfbd' || exit 1
echo "Deploying config...."
rsync -utRz --progress config.xml erdehaan@kova-01.ewi.tudelft.nl:~/sfbd/config.xml || exit 1
echo "Deploying JARs...."
rsync -utRz --include="*.jar" --exclude="*" --progress ./target/scala-2.11/* erdehaan@kova-01.ewi.tudelft.nl:~/sfbd || exit 1
echo "Running Application...."
ssh erdehaan@kova-01.ewi.tudelft.nl 'cd ~/sfbd ;export SPARK_HOME=/data/spark/spark-2.0.1-bin-hadoop2.4; $SPARK_HOME/bin/spark-submit --class "StreamingMapper" --master local[*] --driver-memory 32g --jars "target/scala-2.11/StreamingMapper-assembly-0.1-SNAPSHOT-deps.jar" target/scala-2.11/StreamingMapper-assembly-0.1-SNAPSHOT.jar'
