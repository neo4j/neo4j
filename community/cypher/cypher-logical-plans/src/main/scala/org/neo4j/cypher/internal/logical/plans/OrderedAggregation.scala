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

import org.neo4j.cypher.internal.v4_0.expressions.{Expression, Property}
import org.neo4j.cypher.internal.v4_0.util.attribution.IdGen

/**
  * OrderedAggregation is like Aggregation, except that it relies on the input coming
  * in a particular order, which it can leverage by keeping less state to aggregate at any given time.
  */
case class OrderedAggregation(source: LogicalPlan,
                              groupingExpressions: Map[String, Expression],
                              aggregationExpression: Map[String, Expression],
                              orderToLeverage: Seq[Expression])
                             (implicit idGen: IdGen)
  extends LogicalPlan(idGen) with EagerLogicalPlan with AggregatingPlan with ProjectingPlan {

  override val projectExpressions: Map[String, Expression] = groupingExpressions

  val groupingKeys: Set[String] = groupingExpressions.keySet

  val availableSymbols: Set[String] = groupingKeys ++ aggregationExpression.keySet

}
