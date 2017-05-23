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
import org.apache.spark.mllib.fim.PatternTree.PatternTreeNode

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class PatternTree[Item: ClassTag](ppctree: PPCTree[Item]) extends Serializable {
  val root = new PatternTreeNode
  root.label = ppctree.numFreqItems
  root.firstChild = null
  val equivalentItems = new Array[Int](ppctree.numFreqItems)
  var resultCount: Int = 0
  var nodesetLenSum: Int = 0
  var resultLen: Int = 0
  val result = new Array[Int](ppctree.numFreqItems)
  var nodesetCount: Int = 0
  val freqItemsets: ArrayBuffer[(Array[Item], Long)] = ArrayBuffer.empty[(Array[Item], Long)]
  var numFreqItemsets: Int = 0

  def initialize[Item: ClassTag](itemToRank: Array[(Item, Long)]): Unit = {

    var lastChild: PatternTreeNode = null
    for(i <- (ppctree.numFreqItems - 1) to 0 by -1) {
      if(ppctree.bfCursor > ppctree.bfCurrentSize - ppctree.headTableLen(i) * 3) {
        ppctree.bfRowIndex += 1
        ppctree.bfCursor = 0
        ppctree.bfCurrentSize = 10 * ppctree.bfSize
        ppctree.bf(ppctree.bfRowIndex) = new Array[Int](ppctree.bfCurrentSize)
      }

      val patternTreeNode = new PatternTreeNode
      patternTreeNode.label = i
      patternTreeNode.support = 0
      patternTreeNode.beginInBf = ppctree.bfCursor
      patternTreeNode.length = 0
      patternTreeNode.rowIndex = ppctree.bfRowIndex
      var ni = ppctree.headTable(i)
      while (ni != null) {
        patternTreeNode.support += ni.count
        ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = ni.foreIndex
        ppctree.bfCursor += 1
        ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = ni.backIndex
        ppctree.bfCursor += 1
        ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = ni.count
        ppctree.bfCursor += 1
        patternTreeNode.length += 1
        ni = ni.labelSibling
      }
      if (root.firstChild == null) {
        root.firstChild = patternTreeNode
        lastChild = patternTreeNode
      } else {
        lastChild.next = patternTreeNode
        lastChild = patternTreeNode
      }
    }
  }

  def mine(id: Int, partitioner: Partitioner): ArrayBuffer[(Array[Item], Long)] = {
    var curNode = root.firstChild

    val fromBfCursor = ppctree.bfCursor
    val fromBfRowIndex = ppctree.bfRowIndex
    val fromBfSize = ppctree.bfCurrentSize

    val fromDiffCursor = ppctree.diffCursor
    val fromDiffRowIndex = ppctree.diffRowIndex
    val fromDiffSize = ppctree.diffCurrentSize

    while (curNode != null) {
      if(partitioner.getPartition(curNode.label) == id) {
        traverse(curNode, this.root, 1, 0)

        for (i <- ppctree.bfRowIndex until fromBfRowIndex by -1) {
          ppctree.bf(i) = null
        }
        for (i <- ppctree.diffRowIndex until fromDiffRowIndex by -1) {
          ppctree.diff(i) = null
        }

        ppctree.bfRowIndex = fromBfRowIndex
        ppctree.bfCursor = fromBfCursor
        ppctree.bfCurrentSize = fromBfSize

        ppctree.diffRowIndex = fromDiffRowIndex
        ppctree.diffCursor = fromDiffCursor
        ppctree.diffSize = fromDiffSize
      }
      curNode = curNode.next
    }
    this.freqItemsets
  }

  /**
   * Recursively traverse the pattern tree to find frequent itemsets
   */
  def traverse(
      curNode: PatternTreeNode,
      curRoot: PatternTreeNode,
      level: Int,
      count: Int): Unit = {
    var sibling = curNode.next
    var equivalentCount = count
    var lastChild: PatternTreeNode = null

    while (sibling != null) {

      if (level == 1 &&
        ppctree.itemsetCount((curNode.label - 1) * curNode.label / 2 + sibling.label) >= ppctree.minCount) {
        val equivalentCountTemp = new IntegerByRef(equivalentCount)
        lastChild = is2ItemsetEquivalent(curNode, sibling, lastChild, equivalentCountTemp)
        equivalentCount = equivalentCountTemp.count
      } else if(level > 1) {
        val equivalentCountTemp = new IntegerByRef(equivalentCount)
        lastChild = iskItemsetEquivalent(curNode, sibling, lastChild, equivalentCountTemp)
        equivalentCount = equivalentCountTemp.count
      }
      sibling = sibling.next
    }
    this.resultCount +=  Math.pow(2.0, equivalentCount.toDouble).toInt
    this.nodesetLenSum += Math.pow(2.0, equivalentCount.toDouble).toInt * curNode.length
    this.result(this.resultLen) = curNode.label
    this.resultLen += 1
    genFreqItemsets(curNode, equivalentCount)
    this.nodesetCount += 1

    val fromBfCursor = ppctree.bfCursor
    val fromBfRowIndex = ppctree.bfRowIndex
    val fromBfSize = ppctree.bfCurrentSize

    val fromDiffCursor = ppctree.diffCursor
    val fromDiffRowIndex = ppctree.diffRowIndex
    val fromDiffSize = ppctree.diffCurrentSize

    var child: PatternTreeNode = curNode.firstChild

    while (child != null) {
      traverse(child, curNode, level + 1, equivalentCount)

      for(i <- ppctree.bfRowIndex until fromBfRowIndex by -1) {
        ppctree.bf(i) = null
      }

      for(i <- ppctree.diffRowIndex until fromDiffRowIndex by -1) {
        ppctree.diff(i) = null
      }

      ppctree.bfRowIndex = fromBfRowIndex
      ppctree.bfCursor = fromBfCursor
      ppctree.bfCurrentSize = fromBfSize

      ppctree.diffRowIndex = fromDiffRowIndex
      ppctree.diffCursor = fromDiffCursor
      ppctree.diffCurrentSize = fromDiffSize

      child = child.next
    }
    this.resultLen -= 1
  }

  class IntegerByRef(var count:Int)

  def is2ItemsetEquivalent(
      ni: PatternTreeNode,
      nj: PatternTreeNode,
      node: PatternTreeNode,
      equivalentCount: IntegerByRef): PatternTreeNode = {
    var lastChild = node
    if (ppctree.bfCursor + ni.length * 3 > ppctree.bfCurrentSize) {
      ppctree.bfRowIndex += 1
      ppctree.bfCursor = 0
      ppctree.bfCurrentSize = if(ppctree.bfSize > ni.length * 100) ppctree.bfSize else ni.length * 100
      ppctree.bf(ppctree.bfRowIndex) = new Array[Int](ppctree.bfCurrentSize)
    }

    if (ppctree.diffCursor + ni.length * 3 > ppctree.diffCurrentSize) {
      ppctree.diffRowIndex += 1
      ppctree.diffCursor = 0
      ppctree.diffCurrentSize = if (ppctree.diffSize > ni.length * 100) ppctree.diffSize else ni.length * 100
      ppctree.diff(ppctree.diffRowIndex) = new Array[Int](ppctree.diffCurrentSize)
    }

    var nlNode = new PatternTreeNode()
    nlNode.support = ppctree.itemsetCount((ni.label - 1) * ni.label / 2 + nj.label)
    nlNode.length = 0
    val tempBfCursor = ppctree.bfCursor
    val tempBfRowIndex = ppctree.bfRowIndex
    val tempDiffCursor = ppctree.diffCursor
    val tempDiffRowIndex = ppctree.diffRowIndex
    var pi = ppctree.headTable(ni.label)
    var pj = ppctree.headTable(nj.label)

    var tempBfLength = 0
    var tempDiffLength = 0
    var stop1 = true
    var stop2 = true

    while (pi != null && pj != null) {
      if (pi.backIndex > pj.backIndex) {
        pj = pj.labelSibling
      } else {
        if (pi.backIndex < pj.backIndex && pi.foreIndex > pj.foreIndex) {
          if (stop1 == true) {
            ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = pi.backIndex
            ppctree.bfCursor += 1
            ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = pi.count
            ppctree.bfCursor += 1
            tempBfLength += 1
            if (tempBfLength > 0.5 * ni.length) {
              stop1 = false
            }
          }
          pi = pi.labelSibling
        } else {
          if (stop2 == true) {
            ppctree.diff(ppctree.diffRowIndex)(ppctree.diffCursor) = pi.backIndex
            ppctree.diffCursor += 1
            ppctree.diff(ppctree.diffRowIndex)(ppctree.diffCursor) = pi.count
            ppctree.diffCursor += 1
            tempDiffLength += 1
            if (tempDiffLength > 0.5 * ni.length) {
              stop2 = false
            }
          }
          pi = pi.labelSibling
        }
      }
    }
    if (stop2 == false) {
      nlNode.useDiff = false
      nlNode.length = tempBfLength
      nlNode.beginInBf = tempBfCursor
      nlNode.rowIndex = tempBfRowIndex
      ppctree.diffCursor = tempDiffCursor
    } else {
      nlNode.useDiff = true
      while (pi != null) {
        ppctree.diff(ppctree.diffRowIndex)(ppctree.diffCursor) = pi.backIndex
        ppctree.diffCursor += 1
        ppctree.diff(ppctree.diffRowIndex)(ppctree.diffCursor) = pi.count
        ppctree.diffCursor += 1
        tempDiffLength += 1
        pi = pi.labelSibling
      }
      nlNode.length = tempDiffLength
      nlNode.beginInBf = tempDiffCursor
      nlNode.rowIndex = tempDiffRowIndex
      ppctree.bfCursor = tempBfCursor
    }
    if (ni.support == nlNode.support) {
      equivalentItems(equivalentCount.count) = nj.label
      equivalentCount.count += 1
      if (nlNode.useDiff == true) {
        ppctree.diffCursor = tempDiffCursor
      } else {
        ppctree.bfCursor = tempBfCursor
      }
      if (nlNode != null)
        nlNode = null
    } else {
      nlNode.label = nj.label
      nlNode.firstChild = null
      nlNode.next = null
      if (ni.firstChild == null) {
        ni.firstChild = nlNode
        lastChild = nlNode
      } else {
        lastChild.next = nlNode
        lastChild = nlNode
      }
    }
    lastChild
  }

  def iskItemsetEquivalent(
      ni: PatternTreeNode,
      nj: PatternTreeNode,
      node: PatternTreeNode,
      equivalentCount: IntegerByRef): PatternTreeNode = {
    var lastChild = node

    if (ppctree.bfCursor + ni.length * 2 > ppctree.bfCurrentSize) {
      ppctree.bfRowIndex += 1
      ppctree.bfCursor = 0
      ppctree.bfCurrentSize = if(ppctree.bfSize > ni.length * 100) ppctree.bfSize else ni.length * 100
      ppctree.bf(ppctree.bfRowIndex) = new Array[Int](ppctree.bfCurrentSize)
    }

    if (ppctree.diffCursor + ni.length * 2 > ppctree.diffCurrentSize) {
      ppctree.diffRowIndex += 1
      ppctree.diffCursor = 0
      ppctree.diffCurrentSize = if (ppctree.diffSize > ni.length * 100) ppctree.diffSize else ni.length * 100
      ppctree.diff(ppctree.diffRowIndex) = new Array[Int](ppctree.diffCurrentSize)
    }

    var nlNode = new PatternTreeNode
    nlNode.length = 0
    nlNode.support = 0

    val tempBfCursor = ppctree.bfCursor
    val tempBfRowIndex = ppctree.bfRowIndex
    val tempDiffCursor = ppctree.diffCursor
    val tempDiffRowIndex = ppctree.diffRowIndex

    var cursorI = ni.beginInBf
    var cursorJ = nj.beginInBf
    val rowIndexI = ni.rowIndex
    val rowIndexJ = nj.rowIndex

    var tempBfLength = 0
    var tempDiffLength = 0
    var tempBfSupport = 0
    var tempDiffSupport = 0

    if (ni.useDiff.equals(false) && nj.useDiff.equals(false)) {
      while (cursorI < ni.beginInBf + ni.length * 2 && cursorJ < nj.beginInBf + nj.length * 2) {
        if (ppctree.bf(rowIndexI)(cursorI) == ppctree.bf(rowIndexJ)(cursorJ)) {
          ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = ppctree.bf(rowIndexI)(cursorI)
          ppctree.bfCursor += 1
          ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = ppctree.bf(rowIndexI)(cursorI + 1)
          ppctree.bfCursor += 1
          tempBfSupport += ppctree.bf(rowIndexI)(cursorI + 1)
          tempBfLength += 1
          cursorI += 2
          cursorJ += 2
        } else if (ppctree.bf(rowIndexI)(cursorI) < ppctree.bf(rowIndexJ)(cursorJ)) {
          ppctree.diff(ppctree.diffRowIndex)(ppctree.diffCursor) = ppctree.bf(rowIndexI)(cursorI)
          ppctree.diffCursor += 1
          ppctree.diff(ppctree.diffRowIndex)(ppctree.diffCursor) = ppctree.bf(rowIndexI)(cursorI + 1)
          ppctree.diffCursor += 1
          tempDiffSupport += ppctree.bf(rowIndexI)(cursorI + 1)
          tempDiffLength += 1
          cursorI += 2
        } else {
          cursorJ += 2
        }
      }
      if (tempBfLength < 0.5 * ni.length) {
        nlNode.useDiff = false
        nlNode.length = tempBfLength
        nlNode.beginInBf = tempBfCursor
        nlNode.rowIndex = tempBfRowIndex
        nlNode.support = tempBfSupport
        ppctree.diffCursor = tempDiffCursor
      } else {
        while (cursorI < ni.beginInBf + ni.length * 2) {
          ppctree.diff(ppctree.diffRowIndex)(ppctree.diffCursor) = ppctree.bf(rowIndexI)(cursorI)
          ppctree.diffCursor += 1
          ppctree.diff(ppctree.diffRowIndex)(ppctree.diffCursor) = ppctree.bf(rowIndexI)(cursorI + 1)
          ppctree.diffCursor += 1
          tempDiffSupport += ppctree.bf(rowIndexI)(cursorI + 1)
          tempDiffLength += 1
          cursorI += 2
        }
        nlNode.useDiff = true
        nlNode.length = tempDiffLength
        nlNode.beginInBf = tempDiffCursor
        nlNode.rowIndex = tempDiffRowIndex
        nlNode.support = ni.support - tempDiffSupport
        ppctree.bfCursor = tempBfCursor
      }
    } else if (ni.useDiff.equals(false) && nj.useDiff.equals(true)) {
      while (cursorI < ni.beginInBf + ni.length * 2 && cursorJ < nj.beginInBf + nj.length * 2) {
        if (ppctree.bf(rowIndexI)(cursorI) == ppctree.diff(rowIndexJ)(cursorJ)) {
          ppctree.diff(ppctree.diffRowIndex)(ppctree.diffCursor) = ppctree.bf(rowIndexI)(cursorI)
          ppctree.diffCursor += 1
          ppctree.diff(ppctree.diffRowIndex)(ppctree.diffCursor) = ppctree.bf(rowIndexI)(cursorI + 1)
          ppctree.diffCursor += 1
          tempDiffSupport += ppctree.bf(rowIndexI)(cursorI + 1)
          tempDiffLength += 1
          cursorI += 2
          cursorJ += 2
        } else if (ppctree.bf(rowIndexI)(cursorI) < ppctree.diff(rowIndexJ)(cursorJ)) {
          ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = ppctree.bf(rowIndexI)(cursorI)
          ppctree.bfCursor += 1
          ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = ppctree.bf(rowIndexI)(cursorI + 1)
          ppctree.bfCursor += 1
          tempBfSupport += ppctree.bf(rowIndexI)(cursorI + 1)
          tempBfLength += 1
          cursorI += 2
        } else {
          cursorJ += 2
        }
      }
      if (tempBfLength < 0.5 * ni.length) {
        while (cursorI < ni.beginInBf + ni.length * 2) {
          ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = ppctree.bf(rowIndexI)(cursorI)
          ppctree.bfCursor += 1
          ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = ppctree.bf(rowIndexI)(cursorI + 1)
          ppctree.bfCursor += 1
          tempBfSupport += ppctree.bf(rowIndexI)(cursorI + 1)
          tempBfLength += 1
          cursorI += 2
        }
        nlNode.useDiff = false
        nlNode.length = tempBfLength
        nlNode.beginInBf = tempBfCursor
        nlNode.rowIndex = tempBfRowIndex
        nlNode.support = tempBfSupport
        ppctree.diffCursor = tempDiffCursor
      } else {
        nlNode.useDiff = true
        nlNode.length = tempDiffLength
        nlNode.beginInBf = tempDiffCursor
        nlNode.rowIndex = tempDiffRowIndex
        nlNode.support = ni.support - tempDiffSupport
        ppctree.bfCursor = tempBfCursor
      }
    } else if (ni.useDiff.equals(true) && nj.useDiff.equals(false)) {
      while (cursorI < ni.beginInBf + ni.length * 2 && cursorJ < nj.beginInBf + nj.length * 2) {
        if (ppctree.diff(rowIndexI)(cursorI) == ppctree.bf(rowIndexJ)(cursorJ)) {
          cursorI += 2
          cursorJ += 2
        } else if (ppctree.diff(rowIndexI)(cursorI) < ppctree.bf(rowIndexJ)(cursorJ)) {
          cursorI += 2
        } else {
          ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = ppctree.bf(rowIndexJ)(cursorJ)
          ppctree.bfCursor += 1
          ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = ppctree.bf(rowIndexJ)(cursorJ + 1)
          ppctree.bfCursor += 1
          tempBfSupport += ppctree.bf(rowIndexJ)(cursorJ + 1)
          tempBfLength += 1
          cursorJ += 2
        }
      }
      while (cursorJ < nj.beginInBf + nj.length * 2) {
        ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = ppctree.bf(rowIndexJ)(cursorJ)
        ppctree.bfCursor += 1
        ppctree.bf(ppctree.bfRowIndex)(ppctree.bfCursor) = ppctree.bf(rowIndexJ)(cursorJ + 1)
        ppctree.bfCursor += 1
        tempBfSupport += ppctree.bf(rowIndexJ)(cursorJ + 1)
        tempBfLength += 1
        cursorJ += 2
      }
      nlNode.useDiff = false
      nlNode.support = tempBfSupport
      nlNode.length = tempBfLength
      nlNode.rowIndex = tempBfRowIndex
      nlNode.beginInBf = tempBfCursor
    } else {
      var DNSupport = 0
      while (cursorI < ni.beginInBf + ni.length * 2 && cursorJ < nj.beginInBf + nj.length * 2) {
        if (ppctree.diff(rowIndexI)(cursorI) == ppctree.diff(rowIndexJ)(cursorJ)) {
          cursorI += 2
          cursorJ += 2
        } else if (ppctree.diff(rowIndexI)(cursorI) < ppctree.diff(rowIndexJ)(cursorJ)) {
          cursorI += 2
        } else {
          ppctree.diff(ppctree.diffRowIndex)(ppctree.diffCursor) = ppctree.diff(rowIndexJ)(cursorJ)
          ppctree.diffCursor += 1
          ppctree.diff(ppctree.diffRowIndex)(ppctree.diffCursor) = ppctree.diff(rowIndexJ)(cursorJ + 1)
          ppctree.diffCursor += 1
          nlNode.length += 1
          DNSupport += ppctree.diff(rowIndexJ)(cursorJ + 1)
          cursorJ += 2
        }
      }
      while (cursorJ < nj.beginInBf + nj.length * 2) {
        ppctree.diff(ppctree.diffRowIndex)(ppctree.diffCursor) = ppctree.diff(rowIndexJ)(cursorJ)
        ppctree.diffCursor += 1
        ppctree.diff(ppctree.diffRowIndex)(ppctree.diffCursor) = ppctree.diff(rowIndexJ)(cursorJ + 1)
        ppctree.diffCursor += 1
        nlNode.length += 1
        DNSupport += ppctree.diff(rowIndexJ)(cursorJ + 1)
        cursorJ += 2
      }
      nlNode.useDiff = true
      nlNode.beginInBf = tempDiffCursor
      nlNode.rowIndex = tempDiffRowIndex
      nlNode.support = ni.support - DNSupport
    }
    if (nlNode.support >= ppctree.minCount) {
      if (ni.support == nlNode.support) {
        equivalentItems(equivalentCount.count) = nj.label
        equivalentCount.count += 1
        if (nlNode.useDiff.equals(true)) {
          ppctree.diffCursor = tempDiffCursor
        } else {
          ppctree.bfCursor = tempBfCursor
        }
        if (nlNode != null)
          nlNode = null
      } else {
        nlNode.label = nj.label
        nlNode.firstChild = null
        nlNode.next = null
        if (ni.firstChild == null) {
          ni.firstChild = nlNode
          lastChild = nlNode
        } else {
          lastChild.next = nlNode
          lastChild = nlNode
        }
      }
    }
    lastChild
  }

  def genFreqItemsets(curNode: PatternTreeNode, equivalentCount: Int): Unit ={
    if (curNode.support >= ppctree.minCount) {
      this.numFreqItemsets += 1
      val freqItemset = ArrayBuffer.empty[Item]
      for(i <- 0 until this.resultLen) {
        freqItemset += ppctree.rankToItem.get(this.result(i)).get
      }
      val tuple = (freqItemset.toArray, curNode.support)
      this.freqItemsets += tuple
    }
    if (equivalentCount > 0) {
      val max = 1 << equivalentCount
      for(i <- 1 until max) {
        val freqItemset = ArrayBuffer.empty[Item]
        for(k <- 0 until this.resultLen) {
          freqItemset += ppctree.rankToItem.get(this.result(k)).get
        }
        for(j <- 0 until equivalentCount) {
          val isSet = i & (1 << j)
          if(isSet > 0) {
            freqItemset += ppctree.rankToItem.get(this.equivalentItems(j)).get
          }
        }
        val tuple = (freqItemset.toArray, curNode.support)
        this.freqItemsets += tuple
        this.numFreqItemsets += 1
      }
    }
  }
}

object PatternTree {
  class PatternTreeNode extends Serializable {
    var label: Int = _
    var firstChild: PatternTreeNode = null
    var next: PatternTreeNode = null
    var support: Long = 0
    var beginInBf: Int = -1
    var length: Int = 0
    var rowIndex: Int = 0
    var useDiff: Boolean = false
  }
}
