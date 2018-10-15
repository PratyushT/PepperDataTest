# PepperDataTest
Assignments done for PepperData

I have taken two approaches to solve the given assignment.
1) First is the standard batch processing model. I have read the log entries into case class, formed the dataframe, and written the aggregate sql queries. 
2) Second utilises the structured streaming introduced in the Spark 2.2.0. I have read a kafka stream, and performed aggregations to compute after every minute.

You need to first build the project. Clone the project into your system, and then build the project using the command "mvn clean install"

As instructed I have hardcoded the master to be local. To run the job using spark-submit, you need to provide a properties file containing the following 4 fields (provided here with their sample values and explanations). I have uploaded a sample properties file in the project

mode=batch                                                      #this determines the mode in which the job will run
logfile=D:/PepperDataWorkSpace/PepperData/nginx_logs.txt        #If batch, provide the location of the logs data here
brokers=localhost:9092                                          #If kafkaStreaming, provide the comma separated value of kafka brokers
topic=Sensor_1                                                  #If kafkaStreaming, provide the topic to which it will subscribe to

Once the jar is built, you need to run following commands, as per the relevant scenarios, giving the properties file as command line argument:

For processing the logs in batch mode

spark-submit --class com.pepperData.LogProcessing LogProcessing-0.0.1-SNAPSHOT.jar <propertyFileLocation>
  
Example: spark-submit --class com.pepperData.LogProcessing LogProcessing-0.0.1-SNAPSHOT.jar D:\PepperDataWorkSpace\pepperdata.properties

For processing the logs in streaming mode

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 --class com.pepperData.LogProcessing D:\PepperDataWorkSpace\PepperData\target\LogProcessing-0.0.1-SNAPSHOT.jar <propertyFileLocation>
  
Example: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 --class com.pepperData.LogProcessing D:\PepperDataWorkSpace\PepperData\target\LogProcessing-0.0.1-SNAPSHOT.jar D:\PepperDataWorkSpace\pepperdata.properties
