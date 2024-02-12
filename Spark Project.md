# Spark Project

> Abdelhakim EL Ghayoubi

- The goal of this project is to learn data processing using Spark with practical examples on datasets and also apply programming with Scala. 
- I've provided some references where you can learn about Spark. 
- Make sure you use a compatible version of Spark with Hadoop


- **Tutorials :**
    - [**https://www.javatpoint.com/apache-spark-tutorial**](https://www.javatpoint.com/apache-spark-tutorial)
    - [**https://www.altexsoft.com/blog/apache-spark-pros-cons/**](https://www.altexsoft.com/blog/apache-spark-pros-cons/)
    - [**https://www.toptal.com/spark/introduction-to-apache-spark**](https://www.toptal.com/spark/introduction-to-apache-spark)
    - [**https://www.tutorialspoint.com/apache_spark/index.htm**](https://www.tutorialspoint.com/apache_spark/index.htm)


## Installation

- download & install spark
- navigate to **`spark/conf`** folder
    
    ```bash
    cp spark-env.sh.template spark-env.sh
    ```
    
- `spark/conf/spark-env.sh` → (**set your path**)
    
    ```bash
    export PYSPARK_PYTHON=python3
    export HADOOP_CONF_DIR=/home/dexter/Desktop/hadoop-2.7.3/etc/hadoop
    export YARN_CONF_DIR=/home/dexter/Desktop/hadoop-2.7.3/etc/hadoop
    ```
    
- configure yarn
    
    ```bash
    cd <path to hadoop>/etc/hadoop
    
    gedit yarn-site.xml
    # insert this <replace with your **hostname**>
    <property>
    	<name>yarn.resourcemanager.yourhostname</name>
    	<value>your-machine-youryourhostname</value>
    </property>
    # save & close
    
    cat yarn-site.xml 
    
    <?xml version="1.0"?>
    <!--
      Licensed under the Apache License, Version 2.0 (the "License");
      you may not use this file except in compliance with the License.
      You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
      Unless required by applicable law or agreed to in writing, software
      distributed under the License is distributed on an "AS IS" BASIS,
      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
      See the License for the specific language governing permissions and
      limitations under the License. See accompanying LICENSE file.
    -->
    <configuration>
    
    <!-- Site specific YARN configuration properties -->
    <property>
    	<name>yarn.nodemanager.aux-services</name>
    	<value>mapreduce_shuffle</value>
    </property>
    <property>
    	<name>yarn.resourcemanager.dexter-VirtualBox</name>
    	<value>your-machine-dexter-VirtualBox</value>
    </property>
    </configuration>
    ```
    
- locate to hive
    
    ```bash
    cd <path to hive>/hive/conf
    ```
    
- create file inside hive
    
    ```bash
    touch hive-site.xml
    ```
    
- open it & copy this (replace the version of hive you have)
    
    ```xml
    <configuration>  
     <property>
    <name>hive.server2.enable.doAs</name>
    <value>false</value>
    </property>
      <property>
    <name>spark.sql.hive.metastore.version</name>
    <value>1.2.2</value> 
    </property>
    </configuration>
    ```
    
- make symbolic file in spark conf
    
    ```bash
    ln -s <path to/spark/conf>
    ```
    
- start spark → (**start `hadoop` first**)
    
    ```bash
    start-all.sh
    jps
    # kill the process **runJar**
    kill <runJar_number>
    hdfs dfsadmin -safemode leave
    spark-shell --master yarn
    ```
    
- test this commands
    
    ```bash
    sc.setLogLevel("ERROR")
    
    spark.sql("show databases").show
    
    spark.sql("show tables").show
    
    spark.sql("select * from <table_name>").show
    ```
    
## Part 2

### Datasets **`.csv`**
    
  - file name: hospitalData
    
    ```
    CustomerID,VisitDate,Symptoms
    1,2023-01-15,Fever:Headache:Cough
    2,2023-02-28,Sore Throat:Fever:Fatigue
    3,2023-03-10,Headache:Cough:Cold
    4,2023-04-05,Fever:Runny Nose:Fatigue
    5,2023-05-20,Cough:Headache:Sore Throat
    6,2023-06-08,Runny Nose:Fatigue:Headache
    7,2023-07-17,Fever:Sore Throat:Cough
    8,2023-08-22,Headache:Fever:Fatigue
    9,2023-09-30,Cold:Runny Nose:Cough
    10,2023-10-12,Sore Throat:Fever:Fatigue
    11,2023-11-25,Fever:Headache:Cough
    12,2023-12-03,Runny Nose:Headache:Fatigue
    13,2024-01-08,Fever:Runny Nose:Sore Throat
    14,2024-02-14,Headache:Cough:Cold
    15,2024-03-19,Fatigue:Fever:Runny Nose
    16,2024-04-22,Sore Throat:Headache:Fatigue
    17,2024-05-30,Fever:Runny Nose:Headache
    18,2024-06-05,Cough:Headache:Fatigue
    19,2024-07-18,Fever:Sore Throat:Runny Nose
    20,2024-08-25,Headache:Cough:Fatigue
    ```
    
  - file name: purchaseData
    
    ```bash
    ProductName,PurchasePrice,PurchaseDate
    ProductA,120,2023-01-15
    ProductB,80,2023-02-28
    ProductC,150,2023-03-10
    ProductD,90,2023-04-05
    ProductE,110,2023-05-20
    ProductF,130,2023-06-08
    ProductG,95,2023-07-17
    ProductH,120,2023-08-22
    ProductI,80,2023-09-30
    ProductJ,150,2023-10-12
    ProductK,110,2023-11-25
    ProductL,130,2023-12-03
    ProductM,95,2024-01-08
    ProductN,120,2024-02-14
    ProductO,80,2024-03-19
    ProductP,150,2024-04-22
    ProductQ,110,2024-05-30
    ProductR,130,2024-06-05
    ProductS,95,2024-07-18
    ProductT,120,2024-08-25
    ```
    
  - path to dataset
      
    ```bash
    # path to datasets .csv
    /home/dexter/Desktop/spark_assignements/Part2/hospitalData.csv
    /home/dexter/Desktop/spark_assignements/Part2/purchaseData.csv
    
    # load from local file system by this path
    file:///home/dexter/Desktop/spark_assignements/Part2/hospitalData.csv
    file:///home/dexter/Desktop/spark_assignements/Part2/purchaseData.csv
    
    cd /home/dexter/Desktop/spark_assignements/Part2/
    
    # create dir for data
    hadoop fs -mkdir /spark
    
    # copy the data to hdfs
    hadoop fs -copyFromLocal hospitalData.csv /spark/hospitalData.csv
    hadoop fs -copyFromLocal purchaseData.csv /spark/purchaseData.csv
    ```
        
### Solution

  1. Read the purchase record data as an RDD. Filter the records that have price > 100 and print the RDD.
        
      ```scala
      import spark.implicits._
      
      val df1 = spark.read.option("header", "true").option("inferSchema", "true").csv("/spark/purchaseData.csv")
      df1.where("PurchasePrice > 100").show()
      ```
      
      ```scala
      scala> df1.where("PurchasePrice > 100").show()
      +-----------+-------------+-------------------+
      |ProductName|PurchasePrice|       PurchaseDate|
      +-----------+-------------+-------------------+
      |   ProductA|          120|2023-01-15 00:00:00|
      |   ProductC|          150|2023-03-10 00:00:00|
      |   ProductE|          110|2023-05-20 00:00:00|
      |   ProductF|          130|2023-06-08 00:00:00|
      |   ProductH|          120|2023-08-22 00:00:00|
      |   ProductJ|          150|2023-10-12 00:00:00|
      |   ProductK|          110|2023-11-25 00:00:00|
      |   ProductL|          130|2023-12-03 00:00:00|
      |   ProductN|          120|2024-02-14 00:00:00|
      |   ProductP|          150|2024-04-22 00:00:00|
      |   ProductQ|          110|2024-05-30 00:00:00|
      |   ProductR|          130|2024-06-05 00:00:00|
      |   ProductT|          120|2024-08-25 00:00:00|
      +-----------+-------------+-------------------+
      ```
        
  2. Read the hospital record as an RDD. Create another RDD by removing the duplicates from the symptoms. save this RDD as a file in HDFS.
        
      ```scala
      val dfs = spark.read.option("header", "true").option("inferSchema", "true").csv("/spark/hospitalData.csv")
      val RDD = dfs.rdd
      val symptomsRDD = RDD.map(row => row.getString(2)).distinct()
      val symptomArray = symptomsRDD.take(10)
      symptomArray.foreach(println)
      symptomsRDD.saveAsTextFile("hdfs:///spark/output2.txt")
      ```
      
      ```scala
      scala> symptomArray.foreach(println)
      Fever:Runny Nose:Headache
      Cough:Headache:Sore Throat
      Headache:Cough:Cold
      Sore Throat:Fever:Fatigue
      Cold:Runny Nose:Cough
      Cough:Headache:Fatigue
      Fever:Runny Nose:Fatigue
      Fatigue:Fever:Runny Nose
      Headache:Cough:Fatigue
      Runny Nose:Fatigue:Headache
      
      scala> symptomsRDD.saveAsTextFile("hdfs:///spark/output2.txt")
      
      scala>
      ```
        
  3. Read the purchase record as a dataframe. select only first two columns and show the results.
        
      ```scala
      val df1=spark.read.option("header","true").option("inferSchema","true").csv("/spark/purchaseData.csv")
      df1.select("ProductName", "PurchasePrice").show()
      ```
      
      ```scala
      scala> df1.select("ProductName", "PurchasePrice").show()
      +-----------+-------------+
      |ProductName|PurchasePrice|
      +-----------+-------------+
      |   ProductA|          120|
      |   ProductB|           80|
      |   ProductC|          150|
      |   ProductD|           90|
      |   ProductE|          110|
      |   ProductF|          130|
      |   ProductG|           95|
      |   ProductH|          120|
      |   ProductI|           80|
      |   ProductJ|          150|
      |   ProductK|          110|
      |   ProductL|          130|
      |   ProductM|           95|
      |   ProductN|          120|
      |   ProductO|           80|
      |   ProductP|          150|
      |   ProductQ|          110|
      |   ProductR|          130|
      |   ProductS|           95|
      |   ProductT|          120|
      +-----------+-------------+
      ```
        
  4. Read the hospital record as a dataframe. write a query to find out the number of visits by each patient.
        
      ```scala
      val dfs=spark.read.option("header","true").option("inferSchema","true").csv("/spark/hospitalData.csv")
      dfs.createOrReplaceTempView("visite")
      sql("SELECT `Customer ID` AS ID, COUNT(*) AS number_of_visits FROM visite GROUP BY `Customer ID`").show()
      ```
      
      ```scala
      scala> sql("SELECT `Customer ID` AS ID, COUNT(*) AS number_of_visits FROM visite GROUP BY `Customer ID`").show()
      +---+----------------+                                                          
      | ID|number_of_visits|
      +---+----------------+
      | 12|               1|
      |  1|               1|
      | 13|               1|
      |  6|               1|
      | 16|               1|
      |  3|               1|
      | 20|               1|
      |  5|               1|
      | 19|               1|
      | 15|               1|
      |  9|               1|
      | 17|               1|
      |  4|               1|
      |  8|               1|
      |  7|               1|
      | 10|               1|
      | 11|               1|
      | 14|               1|
      |  2|               1|
      | 18|               1|
      +---+----------------+
      ```
        
  5. Store the results of the above dataframe as a parquet table.
        
      ```scala
        // Alias the column in the SQL query
        val resultDF = sql("SELECT `Customer ID` AS ID, COUNT(*) AS number_of_visits FROM visite GROUP BY `Customer ID`")

        // Save the dataframe as a Parquet table
        resultDF.write.parquet("/spark/visits_parquet_table")
        ```

        ```scala
        scala> val resultDF = sql("SELECT `Customer ID` AS ID, COUNT(*) AS number_of_visits FROM visite GROUP BY `Customer ID`")
        resultDF: org.apache.spark.sql.DataFrame = [ID: int, number_of_visits: bigint]

        scala> 

        scala> // Save the dataframe as a Parquet table

        scala> resultDF.write.parquet("/spark/visits_parquet_table
        ```

        check the **`/spark/`** in `hdfs` file system to check if any parquet table is  created

        ```scala
        dexter@dexter-VirtualBox:~$ hadoop fs -ls /spark/visits_parquet_table
        23/12/21 11:29:34 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
        Found 20 items
        -rw-r--r--   1 dexter supergroup          0 2023-12-21 11:26 /spark/visits_parquet_table/_SUCCESS
        -rw-r--r--   1 dexter supergroup        365 2023-12-21 11:26 /spark/visits_parquet_table/part-00000-18b7d11e-bd51-4c89-943b-20b6e9dc3c84-c000.snappy.parquet
        -rw-r--r--   1 dexter supergroup        660 2023-12-21 11:26 /spark/visits_parquet_table/part-00024-18b7d11e-bd51-4c89-943b-20b6e9dc3c84-c000.snappy.parquet
        -rw-r--r--   1 dexter supergroup        660 2023-12-21 11:26 /spark/visits_parquet_table/part-00043-18b7d11e-bd51-4c89-943b-20b6e9dc3c84-c000.snappy.parquet
        -rw-r--r--   1 dexter supergroup        660 2023-12-21 11:26 /spark/visits_parquet_table/part-00048-18b7d11e-bd51-4c89-943b-20b6e9dc3c84-c000.snappy.parquet
        -rw-r--r--   1 dexter supergroup        690 2023-12-21 11:26 /spark/visits_parquet_table/part-00049-18b7d11e-bd51-4c89-943b-20b6e9dc3c84-c000.snappy.parquet
        -rw-r--r--   1 dexter supergroup        660 2023-12-21 11:26 /spark/visits_parquet_table/part-00051-18b7d11e-bd51-4c89-943b-20b6e9dc3c84-c000.snappy.parquet
        -rw-r--r--   1 dexter supergroup        660 2023-12-21 11:26 /spark/visits_parquet_table/part-00053-18b7d11e-bd51-4c89-943b-20b6e9dc3c84-c000.snappy.parquet
        -rw-r--r--   1 dexter supergroup        660 2023-12-21 11:26 /spark/visits_parquet_table/part-00066-18b7d11e-bd51-4c89-943b-20b6e9dc3c84-c000.snappy.parquet
        -rw-r--r--   1 dexter supergroup        660 2023-12-21 11:26 /spark/visits_parquet_table/part-00069-18b7d11e-bd51-4c89-943b-20b6e9dc3c84-c000.snappy.parquet
        -rw-r--r--   1 dexter supergroup        660 2023-12-21 11:26 /spark/visits_parquet_table/part-00077-18b7d11e-bd51-4c89-943b-20b6e9dc3c84-c000.snappy.parquet
        -rw-r--r--   1 dexter supergroup        690 2023-12-21 11:26 /spark/visits_parquet_table/part-00089-18b7d11e-bd51-4c89-943b-20b6e9dc3c84-c000.snappy.parquet
        -rw-r--r--   1 dexter supergroup        660 2023-12-21 11:26 /spark/visits_parquet_table/part-00102-18b7d11e-bd51-4c89-943b-20b6e9dc3c84-c000.snappy.parquet
        -rw-r--r--   1 dexter supergroup        660 2023-12-21 11:26 /spark/visits_parquet_table/part-00103-18b7d11e-bd51-4c89-943b-20b6e9dc3c84-c000.snappy.parquet
        -rw-r--r--   1 dexter supergroup        660 2023-12-21 11:26 /spark/visits_parquet_table/part-00107-18b7d11e-bd51-4c89-943b-20b6e9dc3c84-c000.snappy.parquet
        -rw-r--r--   1 dexter supergroup        660 2023-12-21 11:26 /spark/visits_parquet_table/part-00122-18b7d11e-bd51-4c89-943b-20b6e9dc3c84-c000.snappy.parquet
        -rw-r--r--   1 dexter supergroup        660 2023-12-21 11:26 /spark/visits_parquet_table/part-00163-18b7d11e-bd51-4c89-943b-20b6e9dc3c84-c000.snappy.parquet
        -rw-r--r--   1 dexter supergroup        660 2023-12-21 11:26 /spark/visits_parquet_table/part-00168-18b7d11e-bd51-4c89-943b-20b6e9dc3c84-c000.snappy.parquet
        -rw-r--r--   1 dexter supergroup        660 2023-12-21 11:26 /spark/visits_parquet_table/part-00174-18b7d11e-bd51-4c89-943b-20b6e9dc3c84-c000.snappy.parquet
        -rw-r--r--   1 dexter supergroup        660 2023-12-21 11:26 /spark/visits_parquet_table/part-00192-18b7d11e-bd51-4c89-943b-20b6e9dc3c84-c000.snappy.parquet
        ```
        
  6. Modify query in '4' to order the results in descending order of the count.
        
      ```scala
      val dfs=spark.read.option("header","true").option("inferSchema","true").csv("/spark/hospitalData.csv")
      dfs.createOrReplaceTempView("visite")
      sql("SELECT `Customer ID` AS ID, COUNT(*) AS number_of_visits FROM visite GROUP BY `Customer ID` ORDER BY number_of_visits DESC").show
      ```
      
      ```scala
      scala> sql("SELECT `Customer ID` AS ID, COUNT(*) AS number_of_visits FROM visite GROUP BY `Customer ID` ORDER BY number_of_visits DESC").show
      +---+----------------+                                                          
      | ID|number_of_visits|
      +---+----------------+
      |  1|               1|
      | 13|               1|
      | 12|               1|
      |  6|               1|
      | 16|               1|
      |  3|               1|
      | 20|               1|
      | 19|               1|
      |  5|               1|
      | 15|               1|
      |  9|               1|
      | 17|               1|
      |  8|               1|
      |  4|               1|
      | 10|               1|
      |  7|               1|
      | 11|               1|
      | 14|               1|
      | 18|               1|
      |  2|               1|
      +---+----------------+
      ```
        
  7. Write a method that takes hospital records dataframe and a number and returns the top n number of symptoms. You have done similar mapreduce job in section 4.8.
        
        ```scala
        import org.apache.spark.sql.{DataFrame, Row}
        import org.apache.spark.sql.functions.desc
        
        def topsymptoms(hospitalRecords: DataFrame, n: Int): Array[Row] = {
          val symptomCounts = hospitalRecords.groupBy("Symptoms").count().sort(desc("count"))
          val topSymptoms = symptomCounts.take(n)
          topSymptoms
        }
        
        // Assuming "dfs" is your DataFrame
        val topSymptomsArray = topsymptoms(dfs, 3)
        topSymptomsArray.foreach(println)
        ```
        
        ```scala
        scala> import org.apache.spark.sql.{DataFrame, Row}
        import org.apache.spark.sql.{DataFrame, Row}
        
        scala> import org.apache.spark.sql.functions.desc
        import org.apache.spark.sql.functions.desc
        
        scala> 
        
        scala> def topsymptoms(hospitalRecords: DataFrame, n: Int): Array[Row] = {
             |   val symptomCounts = hospitalRecords.groupBy("Symptoms").count().sort(desc("count"))
             |   val topSymptoms = symptomCounts.take(n)
             |   topSymptoms
             | }
        topsymptoms: (hospitalRecords: org.apache.spark.sql.DataFrame, n: Int)Array[org.apache.spark.sql.Row]
        
        scala> 
        
        scala> // Assuming "dfs" is your DataFrame
        
        scala> val topSymptomsArray = topsymptoms(dfs, 3)
        topSymptomsArray: Array[org.apache.spark.sql.Row] = Array([Sore Throat:Fever:Fatigue,2], [Fever:Headache:Cough,2], [Headache:Cough:Cold,2])
        
        scala> topSymptomsArray.foreach(println)
        [Sore Throat:Fever:Fatigue,2]
        [Fever:Headache:Cough,2]
        [Headache:Cough:Cold,2]
        
        scala>
        ```
        
  8. Use window operations on the hospital record dataframe to add the number previous visits by a patient to each visit record. This will be 0 for the first visit. Then write a query to list the customers who have visited more than once. Check the sql plan for this query in the web-interface.
        
      ```scala
      import org.apache.spark.sql.expressions.Window
      import org.apache.spark.sql.functions._
      
      val wspec = Window.partitionBy("Customer ID").orderBy("Visit Date")
      val visitsWithPrevious = dfs.withColumn("PreviousVisits", row_number().over(wspec) - 1)
      
      val multipleVisits = visitsWithPrevious.groupBy("Customer ID").agg(count("*").alias("TotalVisits")).where(col("TotalVisits") > 1).select("Customer ID").distinct()
      multipleVisits.show()
      multipleVisits.explain()
      ```
      
      ```scala
      scala> import org.apache.spark.sql.expressions.Window
      import org.apache.spark.sql.expressions.Window
      
      scala> import org.apache.spark.sql.functions._
      import org.apache.spark.sql.functions._
      
      scala> 
      
      scala> val wspec = Window.partitionBy("Customer ID").orderBy("Visit Date")
      wspec: org.apache.spark.sql.expressions.WindowSpec = org.apache.spark.sql.expressions.WindowSpec@7c0e06c7
      
      scala> val visitsWithPrevious = dfs.withColumn("PreviousVisits", row_number().over(wspec) - 1)
      visitsWithPrevious: org.apache.spark.sql.DataFrame = [Customer ID: int, Visit Date: timestamp ... 2 more fields]
      
      scala> 
      
      scala> val multipleVisits = visitsWithPrevious.groupBy("Customer ID").agg(count("*").alias("TotalVisits")).where(col("TotalVisits") > 1).select("Customer ID").distinct()
      multipleVisits: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [Customer ID: int]
      
      scala> multipleVisits.show()
      +-----------+
      |Customer ID|
      +-----------+
      +-----------+
      
      scala> multipleVisits.explain()
      == Physical Plan ==
      *(2) HashAggregate(keys=[Customer ID#213], functions=[])
      +- *(2) HashAggregate(keys=[Customer ID#213], functions=[])
          +- *(2) Project [Customer ID#213]
            +- *(2) Filter (TotalVisits#261L > 1)
                +- *(2) HashAggregate(keys=[Customer ID#213], functions=[count(1)])
                  +- Exchange hashpartitioning(Customer ID#213, 200)
                      +- *(1) HashAggregate(keys=[Customer ID#213], functions=[partial_count(1)])
                        +- *(1) FileScan csv [Customer ID#213] Batched: false, Format: CSV, Location: InMemoryFileIndex[hdfs://localhost:9000/spark/hospitalData.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<Customer ID:int>
      ```
        
  9. Modify the parameters maxPartitionBytes and shuffle.partitions to smaller values and check the query respons for queries in '8'
        
      ```scala
      // Set the maxPartitionBytes configuration
      spark.conf.set("spark.sql.files.maxPartitionBytes", (64 * 1024 * 1024).toString) // 64 megabytes in bytes
      // Set the shuffle partitions configuration
      spark.conf.set("spark.sql.shuffle.partitions", "4")
      ```
        
  10. Create a case class for the purchase record read in '3' and convert the dataframe to a dataset. Filter the records with price > 100.
        
      ```scala
      case class PurchaseRecord(ProductName: String, PurchasePrice: Int, PurchaseDate: String)
      // Read the CSV file into a DataFrame
      val df1 = spark.read.option("header", "true").option("inferSchema", "true").csv("/spark/purchaseData.csv")
      // Create a new PurchaseRecord
      val df2 = PurchaseRecord("tabsil taws", 200, "2023-12-30")
      // Convert the PurchaseRecord to a Dataset
      val dsnew = Seq(df2).toDS
      // Show the contents of the Dataset
      dsnew.show
      // Filter the Dataset based on the PurchasePrice
      val filter = dsnew.filter(record => record.PurchasePrice > 100)
      // Show the filtered Dataset
      filter.show()
      ```
      
      ```scala
      scala> dsnew.show
      +-----------+-------------+------------+
      |ProductName|PurchasePrice|PurchaseDate|
      +-----------+-------------+------------+
      |tabsil taws|          200|  2023-12-30|
      +-----------+-------------+------------+
      
      scala> 
      
      scala> // Filter the Dataset based on the PurchasePrice
      
      scala> val filter = dsnew.filter(record => record.PurchasePrice > 100)
      filter: org.apache.spark.sql.Dataset[PurchaseRecord] = [ProductName: string, PurchasePrice: int ... 1 more field]
      
      scala> 
      
      scala> // Show the filtered Dataset
      
      scala> filter.show()
      +-----------+-------------+------------+
      |ProductName|PurchasePrice|PurchaseDate|
      +-----------+-------------+------------+
      |tabsil taws|          200|  2023-12-30|
      +-----------+-------------+------------+
      ```
      
  11. Create a case class for the hospital records read in '4' Use a map to create another dataset with duplicates removed from list of symptoms.
        - it will be better if the header columns in the csv file does not include **spaces** like this **: “**Customer ID**”**
        
      ```scala
      import org.apache.spark.sql.{Dataset, SparkSession}
      // Case class with matching column names
      case class HospitalRecord(CustomerID: String, VisitDate: String, Symptoms: String)
      val spark = SparkSession.builder.appName("HospitalRecords").getOrCreate()
      val dfs = spark.read.option("header", "true").csv("/spark/hospitalData1.csv")
      // Convert DataFrame to Dataset
      val hospitalRecordsDS: Dataset[HospitalRecord] = dfs.as[HospitalRecord]
      // Define a function to split and remove duplicates
      def removeDuplicates(record: HospitalRecord): Seq[String] = {
        record.Symptoms.split(":").map(_.trim).distinct
      }
      // Apply the function and create a Dataset[String]
      val uniqueSymptomsDS: Dataset[String] = hospitalRecordsDS.flatMap(record => removeDuplicates(record))
      // Show the unique symptoms
      uniqueSymptomsDS.show(false)
      ```
      
      ```scala
      scala> uniqueSymptomsDS.show(false)
      +-----------+
      |value      |
      +-----------+
      |Fever      |
      |Headache   |
      |Cough      |
      |Sore Throat|
      |Fever      |
      |Fatigue    |
      |Headache   |
      |Cough      |
      |Cold       |
      |Fever      |
      |Runny Nose |
      |Fatigue    |
      |Cough      |
      |Headache   |
      |Sore Throat|
      |Runny Nose |
      |Fatigue    |
      |Headache   |
      |Fever      |
      |Sore Throat|
      +-----------+
      only showing top 20 rows
      ```
        
  12. create tables in hive from purchase record (PurchaseRecords) and hospital record (HospitalRecords) data frames
        
      ```scala
      // Read purchase data and create a DataFrame
      val dfs = spark.read.option("header", "true").option("inferSchema", "true").csv("/spark/purchaseData.csv")
      dfs.createOrReplaceTempView("PurchaseRecords")
      // Create a table for purchase data
      spark.sql("CREATE TABLE IF NOT EXISTS PurchaseRecordsTable AS SELECT * FROM PurchaseRecords")
      // Read hospital data and create a DataFrame
      val dfsHospital = spark.read.option("header", "true").option("inferSchema", "true").csv("/spark/hospitalData.csv")
      dfsHospital.createOrReplaceTempView("HospitalRecords")
      // Create a table for hospital data
      spark.sql("CREATE TABLE IF NOT EXISTS HospitalRecordsTable AS SELECT * FROM HospitalRecords")
      spark.sql("show tables").show
      ```
      
      ```scala
      scala> spark.sql("show tables").show
      +--------+--------------------+-----------+
      |database|           tableName|isTemporary|
      +--------+--------------------+-----------+
      | default|hospitalrecordstable|      false|
      | default|purchaserecordstable|      false|
      | default|        sample_table|      false|
      |        |     hospitalrecords|       true|
      |        |     purchaserecords|       true|
      +--------+--------------------+-----------+
      ```
        
  13. Create an UDF that removes the duplicates from colon separated symptoms string. Use this UDF in a sql query on HospitalRecords table.
        
      ```scala
      val dfs= spark.read.option("header", "true").option("inferSchema", "true").csv("/spark/hospitalData.csv")
      dfs.createOrReplaceTempView("HospitalRecords") 
      import org.apache.spark.sql.functions.udf
      val removeDuplicatesUDF = udf((symptom: String) => {
        symptom.split(":").distinct.mkString(":")
      })
      spark.udf.register("removeDuplicates", removeDuplicatesUDF)
      val query = "SELECT *, removeDuplicates(symptom) AS UniqueSymptoms FROM HospitalRecords"
      val result = spark.sql(query)
      result.show()
      ```
      
      ```scala
      scala> result.show(false)
      +-----------+----------+----------------------------+----------------------------+
      |Customer ID|Visit Date|Symptoms                    |UniqueSymptoms              |
      +-----------+----------+----------------------------+----------------------------+
      |1          |2023-01-15|Fever:Headache:Cough        |Fever:Headache:Cough        |
      |2          |2023-02-28|Sore Throat:Fever:Fatigue   |Sore Throat:Fever:Fatigue   |
      |3          |2023-03-10|Headache:Cough:Cold         |Headache:Cough:Cold         |
      |4          |2023-04-05|Fever:Runny Nose:Fatigue    |Fever:Runny Nose:Fatigue    |
      |5          |2023-05-20|Cough:Headache:Sore Throat  |Cough:Headache:Sore Throat  |
      |6          |2023-06-08|Runny Nose:Fatigue:Headache |Runny Nose:Fatigue:Headache |
      |7          |2023-07-17|Fever:Sore Throat:Cough     |Fever:Sore Throat:Cough     |
      |8          |2023-08-22|Headache:Fever:Fatigue      |Headache:Fever:Fatigue      |
      |9          |2023-09-30|Cold:Runny Nose:Cough       |Cold:Runny Nose:Cough       |
      |10         |2023-10-12|Sore Throat:Fever:Fatigue   |Sore Throat:Fever:Fatigue   |
      |11         |2023-11-25|Fever:Headache:Cough        |Fever:Headache:Cough        |
      |12         |2023-12-03|Runny Nose:Headache:Fatigue |Runny Nose:Headache:Fatigue |
      |13         |2024-01-08|Fever:Runny Nose:Sore Throat|Fever:Runny Nose:Sore Throat|
      |14         |2024-02-14|Headache:Cough:Cold         |Headache:Cough:Cold         |
      |15         |2024-03-19|Fatigue:Fever:Runny Nose    |Fatigue:Fever:Runny Nose    |
      |16         |2024-04-22|Sore Throat:Headache:Fatigue|Sore Throat:Headache:Fatigue|
      |17         |2024-05-30|Fever:Runny Nose:Headache   |Fever:Runny Nose:Headache   |
      |18         |2024-06-05|Cough:Headache:Fatigue      |Cough:Headache:Fatigue      |
      |19         |2024-07-18|Fever:Sore Throat:Runny Nose|Fever:Sore Throat:Runny Nose|
      |20         |2024-08-25|Headache:Cough:Fatigue      |Headache:Cough:Fatigue      |
      +-----------+----------+----------------------------+----------------------------+
      ```
        
  14. Write an sql query on PurchaseRecord table that shows maximum purchase price for each product.
        
      ```scala
      val dfs= spark.read.option("header", "true").option("inferSchema", "true").csv("/spark/purchaseData.csv")
      dfs.createOrReplaceTempView("PurchaseRecords") 
      val result = spark.sql("SELECT ProductName, MAX(PurchasePrice) AS MaxPurchasePrice FROM PurchaseRecords GROUP BY ProductName")
      result.show()
      ```
      
      ```scala
      scala> result.show()
      +-----------+----------------+                                                  
      |ProductName|MaxPurchasePrice|
      +-----------+----------------+
      |   ProductL|             130|
      |   ProductK|             110|
      |   ProductJ|             150|
      |   ProductN|             120|
      |   ProductS|              95|
      |   ProductI|              80|
      |   ProductG|              95|
      |   ProductQ|             110|
      |   ProductB|              80|
      |   ProductO|              80|
      |   ProductC|             150|
      |   ProductM|              95|
      |   ProductH|             120|
      |   ProductD|              90|
      |   ProductT|             120|
      |   ProductE|             110|
      |   ProductF|             130|
      |   ProductP|             150|
      |   ProductA|             120|
      |   ProductR|             130|
      +-----------+----------------+
      ```
        
  15. Create a map that gives a list of illnesses for each symptom. For example "fever"->"Flu", "runny nose"->"cold". Use this map to write an UDF that takes a colon separated list of symptoms and suggests an illness as diagnosis. Use this udf with the hospital records to add a first diagnosis illness to each record. Remember to use broadcast variables and serialize the class.
        
        
      ```scala
      import org.apache.spark.sql.SparkSession
      import org.apache.spark.sql.functions.udf
      
      // Initialize Spark session
      val spark = SparkSession.builder.appName("DiagnosisApp").getOrCreate()
      
      // Create a map of symptoms to illnesses
      val symptomIllnessMap = Map("fever" -> "Flu", "runny nose" -> "Cold")
      
      // Broadcast the map for better performance
      val broadcastedSymptomIllnessMap = spark.sparkContext.broadcast(symptomIllnessMap)
      
      // Define the UDF using the broadcasted map
      val diagnosisUDF = udf((symptoms: String) => {
        val mapping = broadcastedSymptomIllnessMap.value
        val symptomsList = symptoms.split(":")
        val illnesses = symptomsList.flatMap(symptom => mapping.get(symptom.toLowerCase))
        if (illnesses.nonEmpty) illnesses.head
        else "Unknown"
      })
      
      // Assuming the column is named "Symptoms" in your DataFrame
      val dfs = spark.read.option("header", "true").option("inferSchema", "true").csv("/spark/hospitalData1.csv")
      dfs.createOrReplaceTempView("HospitalRecords")
      
      // Apply the UDF and create a new DataFrame with the Diagnosis column
      val recordsdfs = dfs.withColumn("Diagnosis", diagnosisUDF($"Symptoms"))
      
      // Show the resulting DataFrame
      recordsdfs.show(false)
      
      // Stop Spark session
      spark.stop()
      ```
      
      ```scala
      scala> recordsdfs.show(false)
      +----------+-------------------+----------------------------+---------+
      |CustomerID|VisitDate          |Symptoms                    |Diagnosis|
      +----------+-------------------+----------------------------+---------+
      |1         |2023-01-15 00:00:00|Fever:Headache:Cough        |Flu      |
      |2         |2023-02-28 00:00:00|Sore Throat:Fever:Fatigue   |Flu      |
      |3         |2023-03-10 00:00:00|Headache:Cough:Cold         |Unknown  |
      |4         |2023-04-05 00:00:00|Fever:Runny Nose:Fatigue    |Flu      |
      |5         |2023-05-20 00:00:00|Cough:Headache:Sore Throat  |Unknown  |
      |6         |2023-06-08 00:00:00|Runny Nose:Fatigue:Headache |Cold     |
      |7         |2023-07-17 00:00:00|Fever:Sore Throat:Cough     |Flu      |
      |8         |2023-08-22 00:00:00|Headache:Fever:Fatigue      |Flu      |
      |9         |2023-09-30 00:00:00|Cold:Runny Nose:Cough       |Cold     |
      |10        |2023-10-12 00:00:00|Sore Throat:Fever:Fatigue   |Flu      |
      |11        |2023-11-25 00:00:00|Fever:Headache:Cough        |Flu      |
      |12        |2023-12-03 00:00:00|Runny Nose:Headache:Fatigue |Cold     |
      |13        |2024-01-08 00:00:00|Fever:Runny Nose:Sore Throat|Flu      |
      |14        |2024-02-14 00:00:00|Headache:Cough:Cold         |Unknown  |
      |15        |2024-03-19 00:00:00|Fatigue:Fever:Runny Nose    |Flu      |
      |16        |2024-04-22 00:00:00|Sore Throat:Headache:Fatigue|Unknown  |
      |17        |2024-05-30 00:00:00|Fever:Runny Nose:Headache   |Flu      |
      |18        |2024-06-05 00:00:00|Cough:Headache:Fatigue      |Unknown  |
      |19        |2024-07-18 00:00:00|Fever:Sore Throat:Runny Nose|Flu      |
      |20        |2024-08-25 00:00:00|Headache:Cough:Fatigue      |Unknown  |
      +----------+-------------------+----------------------------+---------+
      ```
        