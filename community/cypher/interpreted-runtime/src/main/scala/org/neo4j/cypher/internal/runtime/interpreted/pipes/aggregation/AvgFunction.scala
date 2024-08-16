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
import org.neo4j.exceptions.InternalException
import org.neo4j.memory.HeapEstimator.shallowSizeOfInstance
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.utils.ValueMath.incrementalAverage

import java.time.temporal.ChronoUnit

/**
 * AVG computation is calculated using cumulative moving average approach:
 * https://en.wikipedia.org/wiki/Moving_average#Cumulative_moving_average
 *
 * TODO consider combining it with https://en.wikipedia.org/wiki/Kahan_summation_algorithm
 */
class AvgFunction(val value: Expression)
    extends NumericOrDurationAggregationExpression {

  def name = "AVG"

  private var count: Long = 0L

  private var monthsRunningAvg = 0d
  private var daysRunningAvg = 0d
  private var secondsRunningAvg = 0d
  private var nanosRunningAvg = 0d
  protected var avgNumber: Double = 0.0

  override def result(state: QueryState): Value = aggregatingType match {
    case None =>
      Values.NO_VALUE
    case Some(AggregatingNumbers) =>
      Values.doubleValue(avgNumber)
    case Some(AggregatingDurations) =>
      DurationValue.approximate(monthsRunningAvg, daysRunningAvg, secondsRunningAvg, nanosRunningAvg).normalize()
    case _ => throw new InternalException(s"invalid aggregation type $aggregatingType")
  }

  override def apply(data: ReadableRow, state: QueryState): Unit = {
    val vl = value(data, state)
    actOnNumberOrDuration(
      vl,
      number => {
        count += 1
        avgNumber = incrementalAverage(avgNumber, number.doubleValue(), count)
      },
      duration => {
        count += 1
        monthsRunningAvg += (duration.get(ChronoUnit.MONTHS).asInstanceOf[Double] - monthsRunningAvg) / count
        daysRunningAvg += (duration.get(ChronoUnit.DAYS).asInstanceOf[Double] - daysRunningAvg) / count
        secondsRunningAvg += (duration.get(ChronoUnit.SECONDS).asInstanceOf[Double] - secondsRunningAvg) / count
        nanosRunningAvg += (duration.get(ChronoUnit.NANOS).asInstanceOf[Double] - nanosRunningAvg) / count
      },
      state
    )
  }
}

object AvgFunction {
  val SHALLOW_SIZE: Long = shallowSizeOfInstance(classOf[AvgFunction])
}
