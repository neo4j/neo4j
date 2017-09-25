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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.aggregation

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers.TypeSafeMathSupport
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.QueryState
import org.neo4j.values.storable._

/**
 * AVG computation is calculated using cumulative moving average approach:
 * https://en.wikipedia.org/wiki/Moving_average#Cumulative_moving_average
 */
class AvgFunction(val value: Expression)
  extends AggregationFunction
  with TypeSafeMathSupport
  with NumericExpressionOnly {

  def name = "AVG"

  private var count: Long = 0L
  private var sum: OverflowAwareSum[_] = OverflowAwareSum(0)

  override def result(state: QueryState): Value =
    if (count > 0) {
      asNumberValue(sum.value)
    } else {
      Values.NO_VALUE
    }

  override def apply(data: ExecutionContext, state: QueryState) {
    actOnNumber(value(data, state), (number) => {
      count += 1
      val diff = minus(number, asNumberValue(sum.value)) match {
        case v: NumberValue => v
        case _ => throw new InternalException("cannot average non-numbers")
      }
      val next = divide(diff, Values.doubleValue(count.toDouble)) match {
        case v: NumberValue => v
        case _ => throw new InternalException("cannot average non-numbers")
      }
      sum = sum.add(next)
    })
  }
}
