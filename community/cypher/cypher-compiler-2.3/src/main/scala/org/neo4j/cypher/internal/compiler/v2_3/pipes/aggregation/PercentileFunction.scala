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
package org.neo4j.cypher.internal.compiler.v2_3.pipes.aggregation

import org.neo4j.cypher.internal.compiler.v2_3._
import commands.expressions.{Expression, NumericHelper}
import pipes.QueryState

class PercentileContFunction(val value: Expression, val percentile: Expression)
  extends AggregationFunction
  with NumericExpressionOnly
  with NumericHelper {

  def name = "PERCENTILE_CONT"

  private var temp = Vector[Any]()
  private var count:Int = 0
  private var perc:Double = 0

  def result: Any = {
    temp = temp.sortBy((num:Any) => asDouble(num))

    if(perc == 1.0 || count == 1) {
      temp.last
    } else if(count > 1) {
      val floatIdx = perc * (count - 1)
      val floor = floatIdx.toInt
      val ceil = math.ceil(floatIdx).toInt
      if(ceil == floor || floor == count - 1) temp(floor)
      else asDouble(temp(floor)) * (ceil - floatIdx) + asDouble(temp(ceil)) * (floatIdx - floor)
    } else {
      null
    }
  }

  def apply(data: ExecutionContext)(implicit state: QueryState) {
    actOnNumber(value(data), (number) => {
      if(count < 1) perc = asDouble(percentile(data))
      count += 1
      temp = temp :+ number
    })
  }
}

class PercentileDiscFunction(val value: Expression, val percentile: Expression)
  extends AggregationFunction
  with NumericExpressionOnly
  with NumericHelper {

  def name = "PERCENTILE_DISC"

  private var temp = Vector[Any]()
  private var count:Int = 0
  private var perc:Double = 0

  def result: Any = {
    temp = temp.sortBy((num:Any) => asDouble(num))

    if(perc == 1.0 || count == 1) {
      temp.last
    } else if(count > 1) {
      val floatIdx = perc * count
      var idx = floatIdx.toInt
      idx = if(floatIdx != idx || idx == 0) idx
            else idx - 1
      temp(idx)
    } else {
      null
    }
  }

  def apply(data: ExecutionContext)(implicit state: QueryState) {
    actOnNumber(value(data), (number) => {
      if(count < 1) perc = asDouble(percentile(data))
      count += 1
      temp = temp :+ number
    })
  }
}
