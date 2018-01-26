This is a tool to parse Hadoop history server's log. 

BUILD:
mvn package

TEST:
[Windows]:
java -cp target\hadooplogparser-1.0.jar com.shanyu.hadoop.logparser.LogParser src\resource\application_xxx_0001
[Linux]:
java -cp target/hadooplogparser-1.0.jar com.shanyu.hadoop.logparser.LogParser src/resource/application_xxx_0001

HOWTOUSE:
For HDInsight clusters, just download your logs on your home container located in /app-logs/<username>/logs/<appid> to your local machine. Note that <appid> is something like "application_1398212633980_0001". Suppose you downloaded the logs to a folder called c:\mylogs\app2, then you can run:

java -cp hadooplogparser-1.0.jar com.shanyu.hadoop.logparser.LogParser c:\mylogs\app2

Then each of the container logs will be parsed and written to a ".txt" file, along with the original log file.
