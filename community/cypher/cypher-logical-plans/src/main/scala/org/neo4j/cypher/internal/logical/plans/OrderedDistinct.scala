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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.util.attribution.IdGen

/**
  * OrderedDistinct is like Distinct, except that it relies on the input coming
  * * in a particular order, which it can leverage by keeping less state to aggregate at any given time.
  */
case class OrderedDistinct(source: LogicalPlan,
                           groupingExpressions: Map[String, Expression],
                           orderToLeverage: Seq[Expression])
                          (implicit idGen: IdGen) extends LogicalPlan(idGen) with EagerLogicalPlan with ProjectingPlan with AggregatingPlan {

  override val projectExpressions: Map[String, Expression] = groupingExpressions
  override val availableSymbols: Set[String] = groupingExpressions.keySet
}
