/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.runtime.SelectivityTracker.MAX_ROWS_BEFORE_SORT
import org.neo4j.cypher.internal.runtime.SelectivityTracker.MIN_ROWS_BEFORE_SORT
import org.neo4j.util.CalledFromGeneratedCode

class SelectivityTracker(predicatesCount: Int) {

  // These numbers represent position of a predicate in the original planner ordering
  private[this] val order: Array[Int] = Array.from(0 until predicatesCount)

  private[this] val callCounts: Array[Long] = Array.fill(predicatesCount)(0L)
  private[this] val trueCounts: Array[Long] = Array.fill(predicatesCount)(0L)
  private[this] val selectivities: Array[Double] = Array.fill(predicatesCount)(0d)

  private[this] val orderBySelectivity: Ordering[Int] = new Ordering[Int] {
    override def compare(x: Int, y: Int): Int = java.lang.Double.compare(selectivities(x), selectivities(y))
  }

  private[this] var rowsSinceSort: Long = 0L
  private[this] var sortAfter: Long = MIN_ROWS_BEFORE_SORT

  @CalledFromGeneratedCode
  final def getOrder(): Array[Int] = {
    order
  }

  @CalledFromGeneratedCode
  final def onPredicateResult(predicateIndex: Int, isTrue: Boolean): Unit = {
    callCounts(predicateIndex) += 1
    if (isTrue)
      trueCounts(predicateIndex) += 1
  }

  @CalledFromGeneratedCode
  final def onRowFinished(): Unit = {
    rowsSinceSort += 1
    if (rowsSinceSort >= sortAfter) {
      var i = 0
      while (i < predicatesCount) {
        val calls = callCounts(i)
        if (calls > 0) {
          selectivities(i) = trueCounts(i) / calls.toDouble
        }
        i += 1
      }

      order.sortInPlace()(orderBySelectivity)

      rowsSinceSort = 0
      sortAfter = if (sortAfter < MAX_ROWS_BEFORE_SORT) sortAfter * 2 else MAX_ROWS_BEFORE_SORT
    }
  }
}

object SelectivityTracker {
  val MIN_ROWS_BEFORE_SORT: Long = 256
  val MAX_ROWS_BEFORE_SORT: Long = 1_048_576
}
