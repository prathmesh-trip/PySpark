### Big Data
Big data is a collection of data from many different sources and is often describe by five characteristics: volume, value, variety, velocity, and veracity.
- **Volume**  : Size and amounts of data. Example n files (amounts) of XX TB (size)
- **Value**   : Value that can be dreived from the data
- **Variety** : Diversity of data. Can be combination of different types of data unstructured, semi-structured and raw data
- **Velocity**: Speed of storing, managing and retrieving data
- **Veractiy**: Accuracy of data determines the level of confidence of insights from the data.

### How to deal with Big Data

  **Traditional Approach**
  
  The traditional approach comprises of having a centralized resource for storing data. The solution is not very scalable.

  ![image](https://user-images.githubusercontent.com/60221225/179687747-e38d44e0-9b98-40e4-b83e-47e563cbb80d.png)

  **Hadoop**
      
   - **Google's MapReduce** : 
    - Google solved this problem using an algorithm called MapReduce.
    - This algorithm divides the task into small parts and assigns them to many computers, and collects the results from them which when integrated, form the result dataset.
   - MapReduce program is executed in three stages they are:
      - Mapping: Mapper’s job is to process input data.Each node applies the map function to the local data.
      - Shuffle: Here nodes are redistributed where data is based on the output keys.(output keys are produced by map function).
      - Reduce: Nodes are now processed into each group of output data, per key in parallel.
   - 

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

### Spark
Apache Spark is also an open-source software utility 
  - for big data processing
  - Spark splits up tasks across different nodes just like Haddop
  - Performs faster than Hadoop because it uses Random Access Memory (RAM)


### Key Terms
- **Latency** : Data latency is the time it takes for data packets to be stored or retrieved. 
