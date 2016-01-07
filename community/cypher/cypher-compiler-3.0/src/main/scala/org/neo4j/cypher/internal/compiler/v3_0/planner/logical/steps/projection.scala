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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.steps

import org.neo4j.cypher.internal.frontend.v3_0.ast
import org.neo4j.cypher.internal.frontend.v3_0.ast.Expression
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._

object projection {

  def apply(in: LogicalPlan, projs: Map[String, Expression])
           (implicit context: LogicalPlanningContext): LogicalPlan = {

    val (plan, projectionsMap) = PatternExpressionSolver()(in, projs)

    val ids = plan.availableSymbols

    val projectAllCoveredIds: Set[(String, Expression)] = ids.map {
      case IdName(id) => id -> ast.Variable(id)(null)
    }
    val projections: Set[(String, Expression)] = projectionsMap.toSeq.toSet

    if (projections.subsetOf(projectAllCoveredIds) || projections == projectAllCoveredIds)
      context.logicalPlanProducer.planStarProjection(plan, projectionsMap, projs)
    else
      context.logicalPlanProducer.planRegularProjection(plan, projectionsMap, projs)
  }
}
