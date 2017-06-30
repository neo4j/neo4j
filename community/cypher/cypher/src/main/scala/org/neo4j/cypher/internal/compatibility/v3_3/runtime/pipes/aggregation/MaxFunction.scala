/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.aggregation

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryState
import org.neo4j.values.{AnyValue, AnyValues, Values}

trait MinMax extends AggregationFunction {
  def value: Expression
  def keep(comparisonResult: Int): Boolean
  def name: String

  private var biggestSeen: AnyValue = null

  def result(implicit state: QueryState): AnyValue = biggestSeen

  def apply(data: ExecutionContext)(implicit state: QueryState) {
    value(data) match {
      case v if v == Values.NO_VALUE =>
      case x: AnyValue => checkIfLargest(x)
    }
  }

  private def checkIfLargest(value: AnyValue)(implicit qtx: QueryState) {
    if (biggestSeen == null) {
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
