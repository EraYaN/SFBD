@ECHO OFF
rmdir /S /Q .\freq.txt
rmdir /S /Q .\predfreq.txt

%SPARK_HOME%\bin\spark-submit2.cmd --class "WordFreqCounts" --master local[*] target/scala-2.11/wordfreqcounts_2.11-0.1-SNAPSHOT.jar %1
