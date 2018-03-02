/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.storable._
import org.neo4j.values.utils.ValueMath.overflowSafeAdd

/**
 * AVG computation is calculated using cumulative moving average approach:
 * https://en.wikipedia.org/wiki/Moving_average#Cumulative_moving_average
 */
class AvgFunction(val value: Expression)
  extends AggregationFunction
  with NumericExpressionOnly {

  def name = "AVG"

  private var count: Long = 0L
  private var sum: NumberValue = Values.ZERO_INT

  override def result(state: QueryState): Value =
    if (count > 0) sum
    else Values.NO_VALUE

  override def apply(data: ExecutionContext, state: QueryState) {
    actOnNumber(value(data, state), (number) => {
      count += 1
      val diff = number.minus(sum)
      val next = diff.dividedBy(count.toDouble)
      sum = overflowSafeAdd(sum, next)
    })
  }
}
