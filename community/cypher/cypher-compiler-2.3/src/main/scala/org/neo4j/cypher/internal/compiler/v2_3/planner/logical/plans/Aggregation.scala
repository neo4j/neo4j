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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans

import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CardinalityEstimation, PlannerQuery}
import org.neo4j.cypher.internal.frontend.v2_3.helpers.Eagerly

case class Aggregation(left: LogicalPlan,
                       groupingExpressions: Map[String, Expression],
                       aggregationExpression: Map[String, Expression])
                      (val solved: PlannerQuery with CardinalityEstimation) extends LogicalPlan with EagerLogicalPlan {

  def ap(newSolved: PlannerQuery with CardinalityEstimation) = copy()(newSolved)

  val lhs = Some(left)

  def rhs = None

  val groupingKeys = groupingExpressions.keySet.map(IdName(_))

  val availableSymbols = groupingKeys ++ aggregationExpression.keySet.map(IdName(_))

  override def mapExpressions(f: (Set[IdName], Expression) => Expression): LogicalPlan =
    copy(
      groupingExpressions = Eagerly.immutableMapValues[String, Expression, Expression](groupingExpressions, f(left.availableSymbols, _)),
      aggregationExpression = Eagerly.immutableMapValues[String, Expression, Expression](aggregationExpression, f(left.availableSymbols ++ groupingKeys, _))
    )(solved)
}
