### Big Data
Big data is a collection of data from many different sources and is often describe by five characteristics: volume, value, variety, velocity, and veracity.
- **Volume**  : Size and amounts of data. Example n files (amounts) of XX TB (size)
- **Value**   : Value that can be dreived from the data
- **Variety** : Diversity of data. Can be combination of different types of data unstructured, semi-structured and raw data
- **Velocity**: Speed of storing, managing and retrieving data
- **Veractiy**: Accuracy of data determines the level of confidence of insights from the data.


### How to deal with Big Data

  Solution 1 : **Traditional Approach**
  
  The traditional approach comprises of having a centralized resource for storing data. The solution is not very scalable.

  ![image](https://user-images.githubusercontent.com/60221225/179687747-e38d44e0-9b98-40e4-b83e-47e563cbb80d.png)

  Solution 2 : **Hadoop**
      
   - **Google's MapReduce** : 
    - Google solved this problem using an algorithm called MapReduce.
    - This algorithm divides the task into small parts and assigns them to many computers, and collects the results from them which when integrated, form the result dataset.
   - MapReduce program is executed in three stages they are:
      - **Mapping**: Mapper’s job is to process input data.Each node applies the map function to the local data.
      - **Shuffle**: Here nodes are redistributed where data is based on the output keys.(output keys are produced by map function).
      - **Reduce**: Nodes are now processed into each group of output data, per key in parallel.

![image](https://user-images.githubusercontent.com/60221225/179701884-f272d5fb-08e9-477f-9873-ca3fe2485198.png)

  - **Key Motivations behind Hadoop** : 
    - It is expensive to build big servers with heavy configurations that handle large scale processing
    - As an alternate you can tie together many small systems as single functional distributed system 

### Hadoop and Spark
Hadoop and Spark, both developed by the Apache Software Foundation, are widely used open-source frameworks for big data architectures.


### Hadoop
Apache Hadoop is an open-source software utility that 
  - allows users to manage big data sets (from gigabytes to petabytes) 
  - by enabling a network of computers (or “nodes”) to solve vast and intricate data problems. 
  - It is a highly scalable, cost-effective solution that stores and processes structured, semi-structured and unstructured data (e.g., Internet clickstream records, web server logs, IoT sensor data, etc.).


### Hadoop : Key Components
Hadoop contains primarily four modules
- **Hadoop Distributed File System (HDFS)** : Primary data storage system
- **Yet Another Resource Negotiator (YARN)** : Cluster resource mananger, schedules tasks and allocates resources (eg, CPU and memory) to applications
- **Hadoop MapReduce** : Splits big data processing into smaller tasks, distributes across different nodes and runs each tasks
- **Hadoop Common (Hadoop Core)** : Set of libraries and utilies, which other three modules depend on


### Hadoop : Working
- Data is initially divided into directories & files, files divided into blocks of 128MB/64MB
- Files distributed across cluster nodes for processing
- HDFS supervises processing
- Reduce step


### Spark
Apache Spark is also an open-source software utility 
  - for big data processing
  - Spark splits up tasks across different nodes just like Haddop
  - Performs faster than Hadoop because it uses Random Access Memory (RAM)


### Spark : Key Components
Spark contains primarily four modules

- **Spark Core** : 
  - Underlying execution engine
  - Schedules, Dispatches tasks and coordinates I/O operations
- **Spark SQL** : 
  - Gathers information about the data
- **Spark Streaming and Structered Streaming** : 
  - Gathers information about the data
- **Spark Machine Learning Library (MLlib)** : 
  - sklearn for Spark
  - Machine learning algorithms for scalability plus tools for feature selection and building ML pipelines. 
- **GraphX** : 
  - GraphX is the Spark API for graphs and graph-parallel computation. 
  - Includes a growing collection of graph algorithms and builders to simplify graph analytics tasks.


![image](https://user-images.githubusercontent.com/60221225/179708519-d52d3175-57bd-464c-bd5a-e3eeea220308.png)


### Spark v/s Hadoop : Key Difference
- Spark processes and retains data in memory for subsequent steps 
- MapReduce processes data on disk. 
- As a result, for smaller workloads, Spark’s data processing speeds are up to 100x faster than MapReduce.

Other differences between the two are : 
- **1. Performance** : 
 - Spark is faster becauses it uses RAM
 - Hadoop MapReduce stores data on multiple nodes and processes it in batches via MapReduce 
- **2. Cost** : 
 - Spark --> Costlier, because needs more RAM
 - Hadoop--> Cheaper, because it needs memory
- **3. Processing** :
 - Both work in a distributed environments
 - Spark  -->  Ideal for real-time processing
 - Hadoop --> Ideal for batch processing and linear data processing
- **4. Scalability** :
 - Hadoop quickly scales to accommodate the demand via Hadoop Distributed File System (HDFS). 
 - Spark relies on the fault tolerant HDFS for large volumes of data.
- **5. Security** :
 - Hadoop is more secure
 - Spark can integrate with Hadoop to reach a higher security level.
- **6. Machine Leanrning** :
 - Spark is the superior platform in this category because it includes MLlib, which performs iterative in-memory ML computations. 


### Key Terms
- **Latency**    : Time taken for a packet to be transferred across a network (can be one-way or roundtrip, generally roundtip)
 - Higher values of latency ==> More time being taken to transfer files
- **Throughput** : Units of information processed per unit time
- **Bandwidth**  : Bandwidth the number of packets that can transferred throughout the network
  - **Bandwidth v/s Latency** : Bandwidth (x) and Latency (y) have a cause and effect relationship i.e. less bandwidth ==> higher latency (more time for file transfer)
  - **Bandwidth v/s Throughput** : Throughput <= Bandwidth

![image](https://user-images.githubusercontent.com/60221225/179703147-e822726c-d619-4c5f-b40a-1631c78a7a3e.png)

