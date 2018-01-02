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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.StdevFunction
import org.neo4j.cypher.internal.util.v3_4.symbols._

case class Stdev(anInner: Expression) extends AggregationWithInnerExpression(anInner) {
  def createAggregationFunction = new StdevFunction(anInner, false)

  def expectedInnerType = CTNumber

  def rewrite(f: (Expression) => Expression) = f(Stdev(anInner.rewrite(f)))
}

case class StdevP(anInner: Expression) extends AggregationWithInnerExpression(anInner) {
  def createAggregationFunction = new StdevFunction(anInner, true)

  def expectedInnerType = CTNumber

  def rewrite(f: (Expression) => Expression) = f(StdevP(anInner.rewrite(f)))
}

