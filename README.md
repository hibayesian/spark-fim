# Spark-FIM
Spark-FIM is a library of scalable frequent itemset mining algorithms based on Spark. It includes:
  + PHybridFIN - A parallel frequent itemset mining algorithm based on a novel data structure named HybridNodeset to represent itemsets. It achieves a significantly better performance on different datasets when the minimum support decreases comparing to the FP-Growth algorithm which is implemented in Spark MLlib.

# Examples
## Scala API
```scala
val minSupport = 0.85
val numPartitions = 4

val spark = SparkSession
  .builder()
  .appName("PHyrbidFINExample")
  .master("local[*]")
  .getOrCreate()

val schema = new StructType(Array(
  StructField("features", StringType)))
val transactions = spark.read.schema(schema).text("data/chess.csv").cache()
val numTransactions = transactions.count()
val startTime = currentTime
val freqItemsets = new PHybridFIN()
  .setMinSupport(minSupport)
  .setNumPartitions(transactions.rdd.getNumPartitions)
  .setDelimiter(" ")
  .transform(transactions)

val numFreqItemsets = freqItemsets.count()
val endTime = currentTime
val totalTime: Double = endTime - startTime

println(s"====================== PHybridFIN - STATS ===========================")
println(s" minSupport = " + minSupport + s"    numPartition = " + numPartitions)
println(s" Number of transactions: " + numTransactions)
println(s" Number of frequent itemsets: " + numFreqItemsets)
println(s" Total time = " + totalTime/1000 + "s")
println(s"=====================================================================")

spark.stop()
```

# Requirements
Spark-FIM is built against Spark 2.1.1.

# Build From Source
```scala
sbt package
```

# Licenses
Spark-FIM is available under Apache Licenses 2.0.

# Contact & Feedback
If you encounter bugs, feel free to submit an issue or pull request. Also you can mail to:
+ hibayesian (hibayesian@gmail.com).
