@ECHO OFF

set cores=%1
set actor_input=%2
set actresses_input=%3
set driver_memory=%4

IF [%cores%]==[] ( 
	ECHO Using Default for Core number
	set cores=16
)
IF [%actor_input%]==[] ( 
	ECHO Using Default for Actor input
	set actor_input=./input/actors_out.list
)
IF [%actresses_input%]==[] ( 
	ECHO Using Default for Actress input
	set actresses_input=./input/actresses_out.list
)
IF [%driver_memory%]==[] ( 
	ECHO Using Default for Dirver memory
	set driver_memory=16g
)

ECHO Starting Spark with %cores% cores and %driver_memory% driver-memory and the input files: "%actor_input%" and "%actresses_input%"

%SPARK_HOME%bin\spark-submit2.cmd --class "Bacon" --master local[%cores%] --driver-memory %driver_memory% ./target/scala-2.11/bacon_2.11-1.0.jar %cores% %actor_input% %actresses_input%
