/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.values.AnyValue
import org.neo4j.values.utils.ValueMath.overflowSafeAdd

class SumFunction(val value: Expression)
  extends AggregationFunction
    with NumericOrDurationAggregationExpression {

  def name = "SUM"

  override def result(state: QueryState): AnyValue = aggregatingType match {
    case None =>
      sumNumber
    case Some(AggregatingNumbers) =>
      sumNumber
    case Some(AggregatingDurations) =>
      sumDuration
  }

  override def apply(data: ExecutionContext, state: QueryState) {
    val vl = value(data, state)
    actOnNumberOrDuration(vl,
      number => {
        sumNumber = overflowSafeAdd(sumNumber, number)
      },
      duration => {
        sumDuration = sumDuration.add(duration)
      }
    )
  }
}
