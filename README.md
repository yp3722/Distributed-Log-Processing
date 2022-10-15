# StatsForLogs - Yash Pharande


Create a tool to generate various statistics from large log files in a **distributed fashion using map/reduce**

Deployment Video Link : [Movie Link](https://www.youtube.com)


## Features
* Configurable timeintervals hourly or per minute
* Easy to install and run
* Will handle incorrect inputs with imporper formats


## Description of Tasks

### One


In this task output will show the distribution of different types of messages across predefined time intervals & injected string instances of the designated regex pattern for these log message types.

Output Column Format
```
Time_Interval     No_of_Error    No_of_Warn   No_of_Info   No_of_Debug   Count_of_messages_with_regex 
```

Output Rows
```
04:27:00.0 - 04:27:59.9	 0   0   2   0   0
04:28:00.0 - 04:28:59.9	 1   4   7   1   0
04:29:00.0 - 04:29:59.9	 0   2   8   2   1
04:30:00.0 - 04:30:59.9	 1   5   3   0   0
04:31:00.0 - 04:31:59.9	 0   2   7   2   0
04:32:00.0 - 04:32:59.9	 0   3   9   0   1
04:33:00.0 - 04:33:59.9	 0   2   10  0   2
``` 

**eg - for interval 04:29:00 to 04:29:59 we have  0 - Errors, 2 - Warns, 8 - Info, 2 - debug and 1 - regex containing message.**

---
### Two

In this task output will display the number of ERROR messages with injected string instances of the designated regex pattern sorted in descending order according to the count

Output Rows
```
10:00:00 - 10:59:59 	54
09:00:00 - 09:59:59 	28
07:00:00 - 07:59:59 	1
04:00:00 - 04:59:59 	0
05:00:00 - 05:59:59 	0
06:00:00 - 06:59:59 	0
08:00:00 - 08:59:59 	0
19:00:00 - 19:59:59 	0
Invalid Log Messages  	0
```
---
### Three

In this task output will display the total number of different error types(DEBUG, INFO, WARN, ERROR) present in the log files.

Output Rows
```
DEBUG	20468
ERROR	2103
INFO	148887
Invalid Log Messages : 	1
WARN	40543
```
---
### Four

In this task output will display the number of characters in each log message or each log message type that contain the highest number of characters in the detected instances of the designated regex pattern.

Output Rows
```
DEBUG	105
ERROR	84
INFO	105
Invalid Log Messages : 	-1
WARN	105
```

## Prerequisites
This projects requires the followings:
- Java 11
- Apache Hadoop 3.3.4


## How to Run
To run this project:

1. Generate the Jar file by running the following command in program root directory
```
sbt clean compile assembly
```
The Jar can be found in ***target/scala-3.1.3/***

2. Start the local hadoop instance
```
start-all.sh
```

3. Create input directory in hadoop distributed filesystem
```
hadoop fs -mkdir -p <INPUT_DIRECTORY_PATH>
```
5. Copy the log files from your local filesystem to hadoop filesystem
```
hadoop fs -put <PATH_TO_LOGFILES_ON_YOUR_LOCAL_FILESYSTEM> <INPUT_DIRECTORY_PATH>
```
6. Executing Map-Reduce job (For running the command below, ensure to copy the Jar file in the current directory)
```
hadoop jar <GeneratedJarName> com.yash.RunProject <INPUT_DIRECTORY_PATH> <Output_Path>
```
**Note : make sure Output_path is an empty directory**

