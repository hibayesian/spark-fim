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

package org.apache.spark.ml.fim

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasOutputCol}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.mllib.fim
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

private[ml] trait PHybridFINParams extends Params
  with HasFeaturesCol with HasOutputCol {

  /**
    * Sets the minimal support level.
    * Default: 0.3
    * @group expertGetParam
    */
  final val minSupport = new DoubleParam(
    this, "minSupport", "the minimum support threshold", ParamValidators.inRange(0, 1, true, true)
  )
  setDefault(minSupport -> 0.3)

  /** @group expertGetParam */
  def getMinSupport: Double = $(minSupport)

  /**
    * Sets the number of partitions.
    * Default: 1
    * @group expertGetParam
    */
  final val numPartitions = new IntParam(
    this, "numPartitions", "the number of partitions", ParamValidators.ltEq(1)
  )
  setDefault(numPartitions -> 1)

  /** @group expertGetParam */
  def getNumPartitions: Int = $(numPartitions)

  /**
    * Sets the delimiter.
    * Default: ","
    * @group expertGetParam
    */
  final val delimiter = new Param[String](
    this, "delimiter", "delimiter"
  )
  setDefault(delimiter -> ",")

  /** @group expertGetParam */
  def getDelimiter: String = $(delimiter)
}
class PHybridFIN(override val uid: String) extends Transformer with PHybridFINParams {
  def this() = this(Identifiable.randomUID("PHybridFIN"))

  /**
    * Sets the value of param [[minSupport]].
    * Default is 0.3.
    *
    * @group expertSetParam
    */
  def setMinSupport(value: Double): this.type = {
    require(value > 0, "minSupport must be between 0 and 1.")
    set(minSupport, value)
  }

  /**
    * Sets the value of param [[numPartitions]].
    * Default is 1.
    *
    * @group expertSetParam
    */
  def setNumPartitions(value: Int): this.type = {
    require(value > 0, "numPartitions must be a positive number number.")
    set(numPartitions, value)
  }

  /**
    * Sets the value of param [[delimiter]].
    * Default is ",".
    *
    * @group expertSetParam
    */
  def setDelimiter(value: String): this.type = {
    set(delimiter, value)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val session = dataset.sparkSession
    val schema = transformSchema(dataset.schema)
    val input = dataset.select(col($(featuresCol))).rdd.map {
      case Row(s: String) => s.split($(delimiter))
    }

    val rdd = new fim.PHybridFIN()
      .setMinSupport($(minSupport))
      .setNumPartitions($(numPartitions))
      .run(input)
      .map(r => Row(r._1, r._2))

    session.createDataFrame(rdd, schema)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, ${featuresCol}, StringType)

    StructType(Array(
      StructField(
        PHybridFIN.freqItemset,
        DataTypes.createArrayType(StringType, false),
        false),
      StructField(
        PHybridFIN.count,
        LongType,
        false))
    )
  }
}

object PHybridFIN {
  val freqItemset = "freqItemset"
  val count = "count"
}
