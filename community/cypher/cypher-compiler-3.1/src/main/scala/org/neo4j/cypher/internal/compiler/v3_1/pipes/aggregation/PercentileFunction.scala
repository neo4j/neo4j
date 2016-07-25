/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.pipes.aggregation

import org.neo4j.cypher.internal.compiler.v3_1._
import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.{Expression, NumericHelper}
import org.neo4j.cypher.internal.compiler.v3_1.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v3_1.InvalidArgumentException

abstract class PercentileFunction(val value: Expression, val percentile: Expression) extends AggregationFunction
  with NumericExpressionOnly
  with NumericHelper {

  protected var temp = Vector[Any]()
  protected var count: Int = 0
  protected var perc: Double = 0

  def apply(data: ExecutionContext)(implicit state: QueryState) {
    actOnNumber(value(data), (number) => {
      if (count < 1) {
        perc = asDouble(percentile(data))
        if (perc < 0 || perc > 1.0)
          throw new InvalidArgumentException(
            s"Invalid input '$perc' is not a valid argument, must be a number in the range 0.0 to 1.0")
      }
      count += 1
      temp = temp :+ number
    })
  }
}

class PercentileContFunction(value: Expression, percentile: Expression)
  extends PercentileFunction(value, percentile) {

  def name = "PERCENTILE_CONT"

  def result: Any = {
    temp = temp.sortBy((num: Any) => asDouble(num))

    if (perc == 1.0 || count == 1) {
      temp.last
    } else if (count > 1) {
      val floatIdx = perc * (count - 1)
      val floor = floatIdx.toInt
      val ceil = math.ceil(floatIdx).toInt
      if (ceil == floor || floor == count - 1) temp(floor)
      else asDouble(temp(floor)) * (ceil - floatIdx) + asDouble(temp(ceil)) * (floatIdx - floor)
    } else {
      null
    }
  }
}

class PercentileDiscFunction(value: Expression, percentile: Expression)
  extends PercentileFunction(value, percentile) {

  def name = "PERCENTILE_DISC"

  def result: Any = {
    temp = temp.sortBy((num: Any) => asDouble(num))

    if (perc == 1.0 || count == 1) {
      temp.last
    } else if (count > 1) {
      val floatIdx = perc * count
      var idx = floatIdx.toInt
      idx = if (floatIdx != idx || idx == 0) idx
      else idx - 1
      temp(idx)
    } else {
      null
    }
  }
}
