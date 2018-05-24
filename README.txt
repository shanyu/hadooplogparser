This is a tool to parse Hadoop history server's log, also known as Yarn container logs. Hadoop uses a format called TFile to store the logs. A file of TFile format is a container of key value pairs, and it is not readable by humans. This tool can parse TFile and convert them into text files.

BUILD:
  mvn package

TEST:
[Windows]:
  java -jar target\hadooplogparser-1.1.jar -d test\application_xxx_0001 -o target\tmp
[Linux]:
  java -jar target/hadooplogparser-1.1.jar -d test/application_xxx_0001 -o target/tmp

HOWTOUSE:
[Parse Local Files]
For HDInsight clusters, just download your logs on your home container located in /app-logs/<username>/logs/<appid> to your local machine. Note that <appid> is something like "application_1398212633980_0001". Suppose you downloaded the logs to a folder called c:\mylogs\app2, then you can run:

  java -jar target\hadooplogparser-1.1.jar -d c:\mylogs\app2 -o c:\mylogs\app2converted

Then each of the container logs will be parsed and written to a ".txt" file, along with the original log file.

[Download and Parse Logs from WASB]
You can also use this tool to directly download Yarn application logs from Azure Storage account associated with an HDInsight cluster.

  java -jar target\hadooplogparser-1.1.jar -a <storageaccount> -c <containername> -k "<storagekey>" -i application_1525928189873_0003 -o target\application_1525928189873_0003

LOGS
This tool produce output to console, and it also produce a log file hadooplogparser.log for Hadoop logs when calling into Hadoop libraries.

FAQ:
1. I got this error:
"Exception parsing file: wn2-yyyyyy._30050_bad_bcfile, exception: java.io.IOException: Not a valid BCFile."

This means one of the log file is corrupted, but hadooplogparser can still try to parse other log files.

2. I got this exception in hadooplogparser.log on Windows:
"2018-05-23 16:49:32 ERROR Shell:397 - Failed to locate the winutils binary in the hadoop binary path
java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries."

This is benign message on Windows box and you can ignore it.
