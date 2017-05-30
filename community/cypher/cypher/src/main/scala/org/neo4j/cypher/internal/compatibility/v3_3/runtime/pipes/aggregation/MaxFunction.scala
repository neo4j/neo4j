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

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryState
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{Comparer, ExecutionContext}
import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.frontend.v3_3.SyntaxException

trait MinMax extends AggregationFunction with Comparer {
  def value: Expression
  def keep(comparisonResult: Int): Boolean
  def name: String

  private var biggestSeen: Any = null

  def result(implicit state: QueryState): Any = biggestSeen

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
    } else if (keep(compareForOrderability(Some(name), biggestSeen, value))) {
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
