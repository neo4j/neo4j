/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds

object projection {

  def apply(
    in: LogicalPlan,
    projectionsToPlan: Map[LogicalVariable, Expression],
    projectionsToMarkSolved: Option[Map[LogicalVariable, Expression]],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val stillToSolveProjection =
      projectionsLeft(in, projectionsToPlan, context.staticComponents.planningAttributes.solveds)
    val solver = SubqueryExpressionSolver.solverFor(in, context)
    val projectionsMap = stillToSolveProjection.map { case (k, v) => (k, solver.solve(v, Some(k.name))) }
    val plan = solver.rewrittenPlan()

    val ids = plan.availableSymbols

    // The projections that are not covered yet
    val projectionsDiff = filterOutEmptyProjections(projectionsMap, ids)

    if (projectionsDiff.isEmpty) {
      // Note, if keepAllColumns == false there might be some cases when runtime would benefit from planning a projection anyway to get rid of unused columns
      context.staticComponents.logicalPlanProducer.planStarProjection(plan, projectionsToMarkSolved)
    } else {
      context.staticComponents.logicalPlanProducer.planRegularProjection(
        plan,
        projectionsDiff,
        projectionsToMarkSolved,
        context
      )
    }
  }

  /**
   * Given a list of `projections`, and the `availableSymbols` of a plan,
   * filter out projections that simple project an available symbol without renaming it.
   */
  def filterOutEmptyProjections(
    projections: Map[LogicalVariable, Expression],
    availableSymbols: Set[LogicalVariable]
  ): Map[LogicalVariable, Expression] = {
    projections.filter({
      case (x, y) if x == y => !availableSymbols.contains(x)
      case _                => true
    })
  }

  /**
   * Computes the projections that are not yet marked as solved.
   */
  private def projectionsLeft(
    in: LogicalPlan,
    projectionsToPlan: Map[LogicalVariable, Expression],
    solveds: Solveds
  ): Map[LogicalVariable, Expression] = {
    // if we had a previous projection it might have projected something already
    // we only want to project what's left from that previous projection
    val alreadySolvedProjections = solveds.get(in.id).asSinglePlannerQuery.tailOrSelf.horizon match {
      case solvedProjection: QueryProjection => solvedProjection.projections
      case _                                 => Map.empty[LogicalVariable, Expression]
    }
    projectionsToPlan -- alreadySolvedProjections.keys
  }
}
