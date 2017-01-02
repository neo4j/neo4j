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
package org.neo4j.cypher.internal.compiler.v2_2.pipes.aggregation

import org.neo4j.cypher.internal.compiler.v2_2._
import commands.expressions.Expression
import pipes.QueryState

trait MinMax extends AggregationFunction with Comparer {
  def value: Expression
  def keep(comparisonResult: Int): Boolean

  private var biggestSeen: Any = null

  def result: Any = biggestSeen

  def apply(data: ExecutionContext)(implicit state: QueryState) {
    value(data) match {
      case null =>
      case x: Comparable[_] => checkIfLargest(x)
      case _ => throw new SyntaxException("MIN/MAX can only handle values of Comparable type, or null. This was a :" + value)
    }
  }

  private def checkIfLargest(value: Any)(implicit qtx: QueryState) {
    if (biggestSeen == null) {
      biggestSeen = value
    } else if (keep(compare(biggestSeen, value))) {
      biggestSeen = value
    }
  }
}

class MaxFunction(val value: Expression) extends AggregationFunction with MinMax {
  def keep(comparisonResult: Int) = comparisonResult < 0
}

class MinFunction(val value: Expression) extends AggregationFunction with MinMax {
  def keep(comparisonResult: Int) = comparisonResult > 0
}
