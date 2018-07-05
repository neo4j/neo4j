/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.ir.v3_5.QueryProjection
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan
import org.opencypher.v9_0.expressions.{Expression, Variable}

object projection {

  def apply(in: LogicalPlan, projs: Map[String, Expression], context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities): LogicalPlan = {

    // if we had a previous projection it might have projected something already
    // we only want to project what's left from that previous projection
    val alreadySolvedProjections = solveds.get(in.id).tailOrSelf.horizon match {
      case solvedProjection: QueryProjection => solvedProjection.projections.keys
      case _ => Seq.empty
    }
    val stillToSolveProjection = projs -- alreadySolvedProjections

    val (plan, projectionsMap) = PatternExpressionSolver()(in, stillToSolveProjection, context, solveds, cardinalities)

    val ids = plan.availableSymbols

    val projectAllCoveredIds: Set[(String, Expression)] = ids.map(id => id -> Variable(id)(null))
    val projections: Set[(String, Expression)] = projectionsMap.toIndexedSeq.toSet

    // The projections that are not covered yet
    val projectionsDiff = (projections -- projectAllCoveredIds).toMap
    if (projectionsDiff.isEmpty) {
      context.logicalPlanProducer.planStarProjection(plan, projs, context)
    } else {
      context.logicalPlanProducer.planRegularProjection(plan, projectionsDiff, projs, context)
    }
  }
}
