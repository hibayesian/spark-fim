/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml

import org.apache.spark.ml.fim.PHybridFIN
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.compat.Platform._

object PHybridFINSuite {
  def main(args: Array[String]): Unit = {
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
  }
}
