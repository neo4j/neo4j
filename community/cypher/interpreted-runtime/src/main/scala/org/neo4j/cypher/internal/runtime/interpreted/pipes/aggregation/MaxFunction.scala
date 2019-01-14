/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.storable.Values
import org.neo4j.values.{AnyValue, AnyValues}

trait MinMax extends AggregationFunction {
  def value: Expression
  def keep(comparisonResult: Int): Boolean
  def name: String

  private var biggestSeen: AnyValue = Values.NO_VALUE

  override def result(state: QueryState): AnyValue = biggestSeen

  override def apply(data: ExecutionContext, state: QueryState) {
    value(data, state) match {
      case Values.NO_VALUE =>
      case x: AnyValue => checkIfLargest(x)
    }
  }

  private def checkIfLargest(value: AnyValue) {
    if (biggestSeen == Values.NO_VALUE) {
      biggestSeen = value
    } else if (keep(AnyValues.COMPARATOR.compare(biggestSeen, value))) {
      biggestSeen = value
    }
  }
}

class MaxFunction(val value: Expression) extends AggregationFunction with MinMax {
  def keep(comparisonResult: Int) = comparisonResult < 0
  override def name: String = "MAX"
}

class MinFunction(val value: Expression) extends AggregationFunction with MinMax {
  def keep(comparisonResult: Int) = comparisonResult > 0
  override def name: String = "MIN"
}
