**Spark sandbox for skewed data**

Intent to shed light on the skewed data problem encountered from time to time 
on data processed with spark framework along with some possible solution.

_Using Spark 2.3.1 with Scala 2.11.12._

Rdds are used but DataFrames or DataSets could be more appropriate for complex data.\
Here for the sake of simplicity, the data is spread across 16 partitions and generated as tuples (pKey, value) 
of integers and skewed thanks to the `Math.exp` function.\
e.g for partitions 0, 1, 2 and 3: 
`(0,0)
(1,0)
(1,1)
(2,0)
(2,1)
(2,2)
(2,3)
(2,4)
(2,5)
(2,6)
(3,0)
(3,1)
(3,2)
...`
until partition 15 containing 12 millions of values (15, x).\
Data is naturally skewed to the right as the partition values with the same key
grow exponentially until a total of 14 millions entities.

A smaller rdd is generated with the same structure but with only 136 values linearly growing.

A simple left join on the small rdd takes around 2mn30s and the spark UI shows clearly that the last partition
is requires the most resources.

![alt text](http://url/to/img.png)