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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v3_0.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.idp.IDPQueryGraphSolver.RelOrJoin
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.idp.expandSolverStep._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.{LogicalPlanningContext, QueryPlannerKit}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{PatternRelationship, LogicalPlan}
import org.neo4j.cypher.internal.frontend.v3_0.InternalException

object tableInitialiser {
  def apply(qg: QueryGraph, kit: QueryPlannerKit, leaves: Set[LogicalPlan])
           (implicit context: LogicalPlanningContext): Set[(Set[RelOrJoin], LogicalPlan)] = {
    val rels = for (patternRel <- qg.patternRelationships)
      yield {
        val accessPlans = planSinglePattern(qg, patternRel, leaves).map(plan => kit.select(plan, qg))
        val bestAccessor = kit.pickBest(accessPlans).getOrElse(
          throw new InternalException("Found no access plan for a pattern relationship in a connected component. This must not happen."))
        val step: RelOrJoin = Left(patternRel)

        Set(step) -> bestAccessor
      }

    rels
  }

  private def planSinglePattern(qg: QueryGraph, pattern: PatternRelationship, leaves: Set[LogicalPlan])
                               (implicit context: LogicalPlanningContext): Iterable[LogicalPlan] = {
    leaves.flatMap {
      case plan if plan.solved.lastQueryGraph.patternRelationships.contains(pattern) =>
        Set(plan)
      case plan if plan.solved.lastQueryGraph.allCoveredIds.contains(pattern.name) =>
        Set(planSingleProjectEndpoints(pattern, plan))
      case plan =>
        val (start, end) = pattern.nodes
        val leftExpand = planSinglePatternSide(qg, pattern, plan, start)
        val rightExpand = planSinglePatternSide(qg, pattern, plan, end)
        leftExpand.toSet ++ rightExpand.toSet
    }
  }

}
