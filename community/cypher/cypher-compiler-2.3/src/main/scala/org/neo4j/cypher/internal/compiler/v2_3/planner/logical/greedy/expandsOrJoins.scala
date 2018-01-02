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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.greedy

import org.neo4j.cypher.internal.frontend.v2_3.ast.UsingJoinHint
import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{CandidateGenerator, LogicalPlanningContext}

object expandsOrJoins extends CandidateGenerator[GreedyPlanTable] {
  def apply(planTable: GreedyPlanTable, queryGraph: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] = {
    val projectedEndpoints = projectEndpoints(planTable, queryGraph)
    val expansions = expand(planTable, queryGraph)
    val joinsOnTopOfExpands = planJoinsOnTopOfExpands(queryGraph, planTable, expansions)
    val joins = join(planTable, queryGraph)
    projectedEndpoints ++ expansions ++ joinsOnTopOfExpands ++ joins
  }

  private def planJoinsOnTopOfExpands(queryGraph: QueryGraph, planTable: GreedyPlanTable, expansions: Seq[LogicalPlan])
                                     (implicit context: LogicalPlanningContext): Seq[LogicalPlan] = {
    val joinHintsPresent = queryGraph.hints.exists {
      case _: UsingJoinHint => true
      case _ => false
    }

    if (joinHintsPresent) {
      expansions.collect {
        case expansion if hasLeafPlanAsChild(expansion) =>
          val table = planTable + expansion
          join(table, queryGraph)
      }.flatten
    }
    else
      Seq.empty
  }

  private def hasLeafPlanAsChild(plan: LogicalPlan) = plan.lhs.exists(p => p.lhs.isEmpty && p.rhs.isEmpty)
}
