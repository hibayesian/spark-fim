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

package org.apache.spark.mllib.fim

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * :: Experimental ::
  *
  * Model trained by [[PHybridFIN]], which holds frequent itemsets.
  *
  * @param freqItemsets frequent itemset, which is an RDD of [[(Array[Item], Long)]]
  * @tparam Item item type
  */
@Experimental
class FIModel[Item: ClassTag](val freqItemsets: RDD[(Array[Item], Long)]) extends Serializable
