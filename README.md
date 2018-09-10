**Spark sandbox for skewed data**

Intent to shed light on the skewed data problem encountered from time to time 
on data processed with spark framework along with some possible solution.

_Using Spark 2.3.1 with Scala 2.11.12._

**From json file**\
Data is modelized as a bid dataset of `User(departmentId: Int, name: String)`
and a smaller dataset representing only the referential `Department(id, name)` data.

Expected json file is an array of `[{"departmentId":8,"name":"Bowers"}...]` with department last id = 9
One of around 15 millions users is supplied [here](https://drive.google.com/open?id=1yCDV5FiOMKf_h6KFaY86ljTQMSqQhtnI).  
Data in this file is skewed this way:   
`
|departmentId|  count:
|           1|6160524|
|           6|8319480|
|           3|2095600|
|           5|2083120|
|           9|2068560|
|           4|2076880|
|           8|4204720|
|           7|2119520|
|           2|2071596|` \
The approach was to repartition skewed data thanks to a unique id added and then a broadcast of the small 
dataset to the big one. Another possibility could be to spread the small dataset across more partitions generating a new unique key
as describe [here](https://stackoverflow.com/questions/40373577/skewed-dataset-join-in-spark).
Results are quite good (around 2mn for a join with on Department Table) and clause in terms of duration, 
leading to the conclusion that data was most likely not skewed enough.

**From memory**\
Another option is to generate manually skewed data in memory:
Here for the sake of simplicity, the data is spread across 11 to 12 or x of your choice partitions and generated as datasets 
of User(Int, String) and skewed thanks to the `Math.exp` function `Partition[15, exp(15) Users]`.\
e.g for partitions 1 and 2: 
`User(1,Fq7e8jn2UN)
 User(2,NMPAFmjt4u)
 User(2,SBLWWlwwxVf)
...`
until partition 15 containing 12 millions of values User(15, xxx).\
Data is naturally skewed to the right as the partition values with the same key
grow exponentially until a total of 14 millions entities.
For 12 partitions, a simple inner join on the small dataset takes around 15mn (8 cores 8gb ram) and the spark \
UI shows clearly that the last partition requires the most resources.
Repartition for partitions is as follows:
`|departmentId|count|
|           1|    2|  
|           6|  403|
|           3|   20|
|           5|  148|
|           9| 8103|
|           4|   54|
|           8| 2980|
|           7| 1096|
|          10|81900|
|           2|    7|
|           0|    1|`

![job 2](https://ibb.co/f8hi29)

Results take around 2mn for both approach but for few thousand users (11 partitions). Higher nb of partitions requires 
more computing power than available to me.

Similar results for both approach lead to the conclusion that more tests are necessary with more representative datasets
and repartition schemes.


The app is package with `sbt assembly`, it is configured (application.conf) to run locally with 4 cores cpus.
To run it, simply use: \
`spark-submit --conf spark.driver.memory=6g target/scala-2.11/sparkling-assembly-0.1.jar`   
allowing the driver to take up to at least 8Gb is preferable with the file supplied in order to keep everything in memory.
