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
import org.neo4j.cypher.internal.compiler.v2_2.helpers.TypeSafeMathSupport
import pipes.QueryState

class AvgFunction(val value: Expression)
  extends AggregationFunction
  with TypeSafeMathSupport
  with NumericExpressionOnly {

  def name = "AVG"

  private var count: Int = 0
  private var sofar: Any = 0

  def result =
    if (count > 0)
      divide(sofar, count.toDouble)
    else
      null

  def apply(data: ExecutionContext)(implicit state: QueryState) {
    actOnNumber(value(data), (number) => {
      count += 1
      sofar = plus(sofar, number)
    })
  }
}
