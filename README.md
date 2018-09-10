**Spark sandbox for skewed data**

Intent to shed light on the skewed data problem encountered from time to time 
on data processed with spark framework along with some possible solution.

_Using Spark 2.3.1 with Scala 2.11.12._

Here for the sake of simplicity, the data is spread across 16 partitions and generated as datasets 
of User(Int, String) and skewed thanks to the `Math.exp` function.\
e.g for partitions 1 and 2: 
`User(1,Fq7e8jn2UN)
 User(2,NMPAFmjt4u)
 User(2,SBLWWlwwxVf)
...`
until partition 15 containing 12 millions of values User(15, xxx).\
Data is naturally skewed to the right as the partition values with the same key
grow exponentially until a total of 14 millions entities.

A smaller dataset is generated and represents only the referential Department(id, name) data.

For 12 partitions, a simple inner join on the small dataset takes around 15mn (8 cores 8gb ram) and the spark \
UI shows clearly that the last partition requires the most resources.

![alt text](http://url/to/img.png)