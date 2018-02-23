// Name- POOJA REDDY NATHALA
//pnathala@uncc.edu

$ cd Downloads
Assuming that all java files are stored in Downloads and jar files are storing in downloads

Steps 1,2,3 are common to execute any java file on hadoop file system.
Step1: Before you run the sample, you must create input and output locations in HDFS. Use the following commands to create the input directory/user/cloudera/wordcount/input in HDFS: 
$ sudo su hdfs
$ hadoop fs -mkdir /user/cloudera
$ hadoop fs -chown cloudera /user/cloudera
$ exit
$ sudo su cloudera
$ hadoop fs -mkdir /user/cloudera/wordcount /user/cloudera/wordcount/input 
Step2:Put the text files in Cantrbry folder into the input directory using the following command:
$ hadoop fs -put canterbury/* /user/cloudera/wordcount/input
Step3:To Compile any java class we are creating a build directory inside the Downloads
$ mkdir -p build 

Steps to execute the DocWordCount.java
Step4: Use the following command to compile the DocWordCount.java 
$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* DocWordCount.java -d build -Xlint
Step5: Use the following command to create a JAR file for DocWordCount application.
$ jar -cvf docwordcount.jar -C build/ .
Step6: Use the following command to run the DocWordCount application from the JAR file, passing the paths to the input and output directories in HDFS as first and second argument.
$ hadoop jar docwordcount.jar org.myorg.DocWordCount /user/cloudera/wordcount/input /user/cloudera/wordcount/output1
Here we are storing the output in /user/cloudera/wordcount/output1 folder
Step7:To display the output on commandprompt run the following command
$ hadoop fs -cat /user/cloudera/wordcount/output1/*
Step8:To get the hdfs output file to local directory use the following command
$ hadoop fs -get /user/cloudera/wordcount/output1/part-r-00000 /home/cloudera/Desktop/​DocWordCount.out
Step9: If you want to run the sample again, you first need to remove the output directory. Use the following command.
$ hadoop fs -rm -r /user/cloudera/wordcount/output1


Steps to execute the TermFrequency.java
Step4: Use the following command to compile the TermFrequency.java
$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TermFrequency.java -d build -Xlint 
Step5: Use the following command to create a JAR file for TermFrequency application.
$ jar -cvf termfrequency.jar -C build/ .
Step6: Use the following command to run the TermFrequency application from the JAR file, passing the paths to the input and output directories in HDFS as first and second argument.
$ hadoop jar termfrequency.jar org.myorg.TermFrequency /user/cloudera/wordcount/input /user/cloudera/wordcount/output2
Step7:To display the output on commandprompt run the following command
$ hadoop fs -cat /user/cloudera/wordcount/output2/*
Step8:To get the hdfs output file to local directory use the following command
$ hadoop fs -get /user/cloudera/wordcount/output2/part-r-00000 /home/cloudera/Desktop/TermFrequency.out
Step9: If you want to run the sample again, you first need to remove the output directory. Use the following command.
$ hadoop fs -rm -r /user/cloudera/wordcount/output2

Steps to execute the TFIDF.java
Step4: Use the following command to compile the TFIDF.java
$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TFIDF.java -d build -Xlint
Step5: Use the following command to create a JAR file for TFIDF application.
$ jar -cvf tfidf.jar -C build/ .
Step6: Use the following command to run the TFIDF application from the JAR file, passing the paths to the input and output directories in HDFS as first and second argument.
$ hadoop jar tfidf.jar org.myorg.TFIDF /user/cloudera/wordcount/input /user/cloudera/wordcount/output3
Step7:To display the output on commandprompt run the following command
$ hadoop fs -cat /user/cloudera/wordcount/output3/*
Step8:To get the hdfs output file to local directory use the following command
$ hadoop fs -get /user/cloudera/wordcount/output3/part-r-00000 /home/cloudera/Desktop/TFIDF.out
Step9: If you want to run the sample again, you first need to remove the output directory. Use the following command.
$ hadoop fs -rm -r /user/cloudera/wordcount/output3

Steps to execute the Search.java with query input "computer science"
Step4: Use the following command to compile the Search.java
$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Search.java -d build -Xlint 
Step5: Use the following command to create a JAR file for Search application.
$ jar -cvf search.jar -C build/ .
Step6: Use the following command to run the Search application from the JAR file, passing the paths to the input and output directories in HDFS as first and second argument.
$ hadoop jar search.jar org.myorg.Search /user/cloudera/wordcount/output3 /user/cloudera/wordcount/output4 "computer science"
// here we are giving "computer science" as query
Step7:To display the output on commandprompt run the following command
$ hadoop fs -cat /user/cloudera/wordcount/output4/*
Step8:To get the hdfs output file to local directory use the following command
$ hadoop fs -get /user/cloudera/wordcount/output4/part-r-00000 /home/cloudera/Desktop/query1.out
Step9: If you want to run the sample again, you first need to remove the output directory. Use the following command.
$ hadoop fs -rm -r /user/cloudera/wordcount/output4

Steps to execute the Search.java with query input "data analysis"
Step4: Use the following command to compile the Search.java
$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Search.java -d build -Xlint 
Step5: Use the following command to create a JAR file for Search application.
$ jar -cvf search.jar -C build/ .
Step6: Use the following command to run the Search application from the JAR file, passing the paths to the input and output directories in HDFS as first and second argument.
$ hadoop jar search.jar org.myorg.Search /user/cloudera/wordcount/output3 /user/cloudera/wordcount/output42 "data analysis"
// here we are giving "data analysis" as query
Step7:To display the output on commandprompt run the following command
$ hadoop fs -cat /user/cloudera/wordcount/output42/*
Step8:To get the hdfs output file to local directory use the following command
$ hadoop fs -get /user/cloudera/wordcount/output42/part-r-00000 /home/cloudera/Desktop/query2.out
Step9: If you want to run the sample again, you first need to remove the output directory. Use the following command.
$ hadoop fs -rm -r /user/cloudera/wordcount/output42

