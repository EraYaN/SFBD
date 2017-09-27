@echo off

%SPARK_HOME%bin\spark-submit2.cmd --class "Twitterstats" target/scala-2.11/Twitterstats_2.11-0.1-SNAPSHOT.jar
