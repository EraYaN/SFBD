@ECHO OFF

set driver_memory=%1

IF [%driver_memory%]==[] ( 
	ECHO Using Default for Dirver memory
	set driver_memory=30g
)

ECHO Starting Spark with %driver_memory% driver-memory.

%SPARK_HOME%bin\spark-submit2.cmd --class "StreamingMapper" --master local[*] --driver-memory %driver_memory% target/scala-2.11/StreamingMapper-assembly-0.1-SNAPSHOT.jar
