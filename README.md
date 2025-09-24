# Project Template - Top-K Hadoop Example. 

# Running on Laptop     ####

Prerequisite:

- Maven 3

- JDK 1.6 or higher

- (If working with eclipse) Eclipse with m2eclipse plugin installed


The java main class is:

edu.cs.utexas.HadoopEx.WordCountTopKDriver 

Input file:  Book-Tiny.txt  

Specify your own Output directory like 

task 1: output/task-r-000000
ATL	346836
ORD	285884
DFW	239551

task 2: delayratiooutput/task-r-000000
F9	13.350858345331709
UA	14.435441010805953
NK	15.944765880783688

# Running:




## Create a JAR Using Maven 

To compile the project and create a single jar file with all dependencies: 
	
```mvn clean package```


## Run your application

Inside your shell with Hadoop

Running as Java Application:

```java -jar target/topKHadoop-0.1-SNAPSHOT-jar-with-dependencies.jar SOME-Text-Fiel.txt  intermediatefolder  output  ``` 

For example 
```java -jar target/topKHadoop-0.1-SNAPSHOT-jar-with-dependencies.jar 20-news-same-line.txt  intermediatefolder  output  ``` 

Or has hadoop application

```hadoop jar your-hadoop-application.jar edu.cs.utexas.HadoopEx.WordCountTopKDriver arg0 arg1 arg2 ```


