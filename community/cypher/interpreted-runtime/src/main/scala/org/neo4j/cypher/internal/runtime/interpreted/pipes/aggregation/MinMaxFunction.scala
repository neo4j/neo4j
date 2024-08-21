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
package org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.memory.HeapEstimator.shallowSizeOfInstance
import org.neo4j.values.AnyValue
import org.neo4j.values.AnyValues
import org.neo4j.values.storable.Values

trait MinMax extends AggregationFunction {
  def value: Expression
  def keep(comparisonResult: Int): Boolean
  def name: String

  private var biggestSeen: AnyValue = Values.NO_VALUE

  override def result(state: QueryState): AnyValue = biggestSeen

  override def apply(data: ReadableRow, state: QueryState): Unit = {
    val x = value(data, state)
    if (x ne Values.NO_VALUE) {
      checkIfLargest(x)
    } else {
      onNoValue(state)
    }
  }

  private def checkIfLargest(value: AnyValue): Unit = {
    if (biggestSeen eq Values.NO_VALUE) {
      biggestSeen = value
    } else if (keep(AnyValues.COMPARATOR.compare(biggestSeen, value))) {
      biggestSeen = value
    }
  }
}

class MaxFunction(val value: Expression) extends AggregationFunction with MinMax {
  def keep(comparisonResult: Int): Boolean = comparisonResult < 0
  override def name: String = "MAX"
}

object MaxFunction {
  val SHALLOW_SIZE: Long = shallowSizeOfInstance(classOf[MaxFunction])
}

class MinFunction(val value: Expression) extends AggregationFunction with MinMax {
  def keep(comparisonResult: Int): Boolean = comparisonResult > 0
  override def name: String = "MIN"
}

object MinFunction {
  val SHALLOW_SIZE: Long = shallowSizeOfInstance(classOf[MinFunction])
}
