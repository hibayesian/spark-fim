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

import org.apache.spark.Partitioner
import org.apache.spark.mllib.fim.PPCTree.PPCTreeNode

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.control.Breaks

class PPCTree[Item: ClassTag](
    val numFreqItems: Int,
    val minCount: Long,
    val rankToItem: Map[Int, Item]) extends Serializable {

  val root: PPCTreeNode = new PPCTreeNode()
  var itemsetCount = new Array[Int]((this.numFreqItems - 1) * this.numFreqItems / 2)
  var PPCTreeNodeCount = 0
  var nodesetRowIndex = 0
  var firstNodesetBegin = 0

  var resultLen = 0
  val result = Array[Int](numFreqItems)
  var headTable: Array[PPCTreeNode] = null
  var headTableLen: Array[Int] = null

  var bfCursor = 0
  var bfRowIndex = 0
  val bfSize = 100000
  var bfCurrentSize = this.bfSize * 10
  val bf = new Array[Array[Int]](100000)
  bf(0)= new Array[Int](bfCurrentSize)

  var diffCursor = 0
  var diffRowIndex = 0
  var diffSize = 1000000
  var diffCurrentSize = this.diffSize * 10
  val diff = new Array[Array[Int]](100000)
  diff(0) = new Array[Int](diffCurrentSize)

  def add(t: Array[Int]): this.type = {
    var curPos = 0
    var curRoot = root
    var rightSibling: PPCTreeNode = null

    val outerLoop = new Breaks()
    outerLoop.breakable(
      while (curPos != t.size) {
        var child = curRoot.firstChild
        val innerLoop = new Breaks()
        innerLoop.breakable(
          while (child != null) {
            if (child.label == t(curPos)) {
              curPos += 1
              child.count += 1
              curRoot = child
              innerLoop.break()
            }
            if (child.rightSibling == null) {
              rightSibling = child
              child = null
              innerLoop.break()
            }
            child = child.rightSibling
          }
        )
        if (child == null) {
          outerLoop.break()
        }
      }
    )

    for (i <- curPos until t.size) {
      val node = new PPCTreeNode()
      node.label = t(i)
      if (rightSibling != null) {
        rightSibling.rightSibling = node
        rightSibling = null
      } else {
        curRoot.firstChild = node
      }
      node.rightSibling = null
      node.firstChild = null
      node.parent = curRoot
      node.count = 1
      curRoot = node
      PPCTreeNodeCount += 1
    }
    this
  }

  def genNodesets(id: Int, partitioner: Partitioner): this.type = {
    this.headTable = new Array[PPCTreeNode](this.numFreqItems)
    this.headTableLen = new Array[Int](this.numFreqItems)

    val tempHead = new Array[PPCTreeNode](this.numFreqItems)
    var curRoot = root.firstChild
    var pre = 0
    var last = 0
    while (curRoot != null) {
      curRoot.foreIndex = pre
      pre += 1

      if (headTable(curRoot.label) == null) {
        headTable(curRoot.label) = curRoot
        tempHead(curRoot.label) = curRoot
      } else {
        tempHead(curRoot.label).labelSibling = curRoot
        tempHead(curRoot.label) = curRoot
      }
      headTableLen(curRoot.label) += 1

      if (partitioner.getPartition(curRoot.label) == id) {
        var temp: PPCTreeNode = curRoot.parent
        while(temp.label != -1) {
          this.itemsetCount(curRoot.label * (curRoot.label - 1) /  2 + temp.label) += curRoot.count
          temp = temp.parent
        }
      }

      if (curRoot.firstChild != null) {
        curRoot = curRoot.firstChild
      } else {
        curRoot.backIndex = last
        last += 1
        if (curRoot.rightSibling != null) {
          curRoot = curRoot.rightSibling
        } else {
          curRoot = curRoot.parent
          val loop = new Breaks()
          loop.breakable(
            while(curRoot != null) {
              curRoot.backIndex = last
              last += 1
              if(curRoot.rightSibling != null) {
                curRoot = curRoot.rightSibling
                loop.break()
              }
              curRoot = curRoot.parent
            }
          )
        }
      }
    }

    this
  }

  def mine(rankToCount: Array[(Int, Long)], id: Int, partitioner: Partitioner): ArrayBuffer[(Array[Item], Long)] = {
    val patternTree = new PatternTree(this)
    patternTree.initialize(rankToCount)
    patternTree.mine(id, partitioner)
  }

}

object PPCTree {
  class PPCTreeNode extends Serializable {
    var label: Int = -1
    var firstChild: PPCTreeNode = null
    var rightSibling: PPCTreeNode = null
    var labelSibling: PPCTreeNode = null
    var parent: PPCTreeNode = null
    var count: Int = -1
    var foreIndex: Int = -1
    var backIndex: Int = -1
  }
}
