# bigmac

## Inputs
- **Industry Standard Artist/Band names file** - a file in HDFS that has a list of 50,000 artist and band names in the music industry standard form.
 
 |Artist/Band name|
 |:---: |
 |The Doors|
 |Led Zeppelin|
 |Stevie Wonder|
 |The Beatles|
 |Ben Harper|
 |Ben Harper & The Innocent Criminals|
 |Foo Fighters|

- **Input Data on distributed file system** - The input file is a tab separated list containing:
  * Artist and Band names  
  * A Timestamp (in unixtime in seconds since the Unix epoch)
  * The number of plays at the given timestamp

 |Artist/Band name | Timestamp| Play Count| 
 |--- |:---: |---: |
 |Doors, The| 1362620749| 50|
 |Led Zeppelin| 1362620750| 12|
 |Stevie Wonder| 1362620749| 5|
 |Beatles| 1362620750| 1|
 |The Beatles| 1362620750| 250|
 |Ben Harper| 1362620749| 3|
 |Ben Harper & The Innocent Criminals| 1362620749| 10|
 |Ben Harper and the Innocent Criminals| 1362620749| 1|
 |The Foo Fighters| 1362620749| 1|
 |Foo Bar Fighters| 1362620749| 1|
 |Bar Foo Fighters| 1362620749| 1|

## The Problem Statement 
- Generate total count(Aggregate total of all the plays per artist/band per day) of the plays for an artist or band by day.
- The data in Artist/Band name column can be mangled with various combinations like punctuations, determiners(missing "The" at the beginning of the string), conjunctions("and" not represented symbolically i.e., "&") etc.  
- Each of the rows in the input file would contribute to the totals if the artist/band
name can be matched. The items that don’t match an artist or band name after solving for the above cases can be ignored. 
- The timestamp field needs to be transformed to date string in "YYYY-mm-dd" format

## Rules to match the Industry Standard form of Artist/Band names
- The parser for "Artist/Band name" field must address the following rules to match the data in artist/band name column to the Industry Standard form
  * Punctuations like (!, ?, ., ', ") should be removed.
  * If a comma(,) appears in the bandname or artist name, needs to be addressed in the following cases:
    * if removal of comma results in a match in the Industry Standard reference dataset, return the resultant string.
    * else remove the comma and interchange the position of words occurring before the comma with the ones after the comma. Return the resultant string if a match is found in the Industry Standard reference dataset.
  * The presence of the conjunction "and" should be replaced with its symbollic equivalent "&".
  * Add missing determiners like "The" at the beginning of the string that would have a potential match in the Industry Standard reference dataset.
  * Ignore the items which cannot be found in the Industry reference dataset after all the above rules are applied and a match coudn't be found.

## Solution - Using Apache Spark(Scala API) with RDD's and DataFrame
- Read the input data into an RDD with custom partition count depending on the resources available on the cluster
- Read the Industry Standard reference file into an RDD and convert it to a Map[String, Int] datatype. Broadcast this Map to make it available for all the nodes processing the input data.
- Convert the epoch timestamp field into a date string with (YYYY-mm-dd) format.
- Parse the Artist/Band name ("Title") field according the Rules defined above.
- Transform the resultant RDD into a Pair RDD with Keys as Artist/Band name and Date string with Value as the number of Plays at a given timestamp.
- Reduce the Pair RDD to aggregate the play count per Artist/Band per Date.
- Transform the resultant Pair RDD into a String RDD. 
- Convert the RDD to a DataFrame with appropriate schema. This DataFrame can be used to perform complex aggregations using SQL like syntax with the benifit of optimizations that the "Catalyst optimizer" and "Tungsten execution engine" of Apache Spark provide out of the box. 
- Store the resultant DataFrame as parquet files partitioned by "Date" at the top level and "Artist/Band name" in the second level(Optional). The partitioning strategy can be decided based based on the heuristics of the input dataset like Number of distinct Artist/Bands, Average number of records per a given timeframe etc. The parition strategy must be carefully chosen to optimize for small files problem, query pattern on the resultant dataset and data rentention policies. 

### Advantages of this Solution
- The hybrid approach of using RDD's and DataFrame gives the flexibility to work at a granular level for unstructured and structured datasets.

## How to Run the App

- **Building from source**
  * Navigate to the directory containing `build.sbt` file and run the following command to build the jar file

    ``` sbt clean package```

  * Use the command below to deploy the application
  
    ```
      /bin/spark-submit \
          --class com.yeshesmeka.plays.ArtistBandPlayDataProcessor \
          --master <master-url> \
          --deploy-mode cluster \
          --conf <key>=<value> \
          <application-jar> \
          <inputFilePath>\
          <standard-reference-file-path>\
          <output-dir-path>\
          <no-of-partitions-input-rdd>
                
       ```

- **Command Line Args** 
  * Input Data path - args(0)
  * Industry Standard Reference file Path - args(1)
  * Output path - args(2)
  * Number of Partitions for the RDD - args(3)


## Cases not covered by the Solution
- If the Artist/Band name contains more than one comma(,) character, the parser doesn't recognize the appropriate arragement of the words in the string.
 > "Foo, Bar, The, Fighters" will not match "The Foo Bar Fighters"

- If the Artist/Band name string has words jumbled in a random manner, that record will be ignored from the resultant dataset.
 > "Fighters Foo The Bar" will not match "The Foo Bar Fighters"

- If the Artist/Band name doesn't have a match in the Industry Standard reference dataset after all the rules defined above are applied, the respective record will be discarded from the resultant dataset.

## Alternate Approach/Solution strategy
- **Apache Hive** - this problem can be solved with Apache Hive using custom User-Defined functions to parse the Artist/Band name field. 
  * Create an external table to read the Industry Standard reference dataset
  * Store the input data in a staging location.
  * Build a external staging table to read from the above location. 
  * Create custom User-Defined function that would parse the Artist/Band name field
  * Use the in-built User-Defined function to transform the epoch time to date string
  * Use joins to filter out the records in the staging table that dont have a match for the Artisit/Band name in the reference dataset.
  * Insert data from staging table to a final table with partitions on date field and artist/band name field(optional) in the desired file format and compression codec.