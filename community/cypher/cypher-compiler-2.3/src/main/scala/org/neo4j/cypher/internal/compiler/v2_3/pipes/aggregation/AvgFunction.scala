/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_3.helpers.TypeSafeMathSupport
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState

class AvgFunction(val value: Expression)
  extends AggregationFunction
  with TypeSafeMathSupport
  with NumericExpressionOnly {

  def name = "AVG"

  private var count: Int = 0
  // avg computation is performed using Kahan's summation algorithm
  private var sum = 0d
  private var c = 0d

  def result =
    if (count > 0) {
      sum
    } else {
      null
    }

  def apply(data: ExecutionContext)(implicit state: QueryState) {
    actOnNumber(value(data), (number) => {
      count += 1
      val add = (number.doubleValue() - sum) / count.toDouble

      val y = add - c
      val t = sum + y
      c = (t - sum) - y
      sum = t
    })
  }
}
