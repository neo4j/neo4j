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
package org.neo4j.cypher.internal.v3_4.logical.plans

import org.neo4j.cypher.internal.v3_4.expressions.Expression
import org.neo4j.cypher.internal.ir.v3_4.{CardinalityEstimation, IdName, PlannerQuery}

/**
  * Aggregation is a more advanced version of Distinct, where source rows are grouped by the
  * values of the groupingsExpressions. When the source is fully consumed, one row is produced
  * for every group, containing the values of the groupingExpressions for that row, as well as
  * aggregates computed on all the rows in that group.
  *
  * If there are no groupingExpressions, aggregates are computed over all source rows.
  */
case class Aggregation(source: LogicalPlan,
                       groupingExpressions: Map[String, Expression],
                       aggregationExpression: Map[String, Expression])
                      (val solved: PlannerQuery with CardinalityEstimation) extends LogicalPlan with EagerLogicalPlan {

  val lhs = Some(source)

  def rhs = None

  val groupingKeys: Set[IdName] = groupingExpressions.keySet.map(IdName(_))

  val availableSymbols: Set[IdName] = groupingKeys ++ aggregationExpression.keySet.map(IdName(_))
}
