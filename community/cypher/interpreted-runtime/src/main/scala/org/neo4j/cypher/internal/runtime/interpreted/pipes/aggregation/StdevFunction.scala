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
package org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, NumericHelper}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

class StdevFunction(val value: Expression, val population:Boolean)
  extends AggregationFunction
  with NumericExpressionOnly
  with NumericHelper {

  def name = if (population) "STDEVP" else "STDEV"

  // would be cool to not have to keep a temporary list to do multiple passes
  // this will blow up RAM over a big data set (not lazy!)
  // but I don't think it's currently possible with the way aggregation works
  private var temp = Vector[Double]()
  private var count:Int = 0
  private var total:Double = 0

  override def result(state: QueryState): AnyValue = {
    if(count < 2) {
      Values.ZERO_FLOAT
    } else {
      val avg = total/count
      val variance = if(population) {
        val sumOfDeltas = temp.foldLeft(0.0)((acc, e) => {val delta = e - avg; acc + (delta * delta) })
        sumOfDeltas / count
      } else {
        val sumOfDeltas = temp.foldLeft(0.0)((acc, e) => {val delta = e - avg; acc + (delta * delta) })
        sumOfDeltas / (count - 1)
      }
      Values.doubleValue(math.sqrt(variance))
    }
  }

  override def apply(data: ExecutionContext, state: QueryState) {
    actOnNumber(value(data, state), (number) => {
      count += 1
      total += number.doubleValue()
      temp = temp :+ number.doubleValue()
    })
  }
}
