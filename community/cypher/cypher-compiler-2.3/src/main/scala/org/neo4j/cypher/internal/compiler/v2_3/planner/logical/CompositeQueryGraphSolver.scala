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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.greedy.GreedyPlanTable
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.frontend.v2_3.HintException

import scala.util.{Failure, Success, Try}

class CompositeQueryGraphSolver(solver1: TentativeQueryGraphSolver, solver2: TentativeQueryGraphSolver,
                                val config: QueryPlannerConfiguration = QueryPlannerConfiguration.default)
  extends TentativeQueryGraphSolver {

  assert(config == solver2.config)
  assert(solver1.config == solver2.config)

  // NOTE: we assume that the PlanTable type is the same between the 2 solvers
  def emptyPlanTable: GreedyPlanTable = GreedyPlanTable.empty

  def tryPlan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]) = {
    val pickBest = config.pickBestCandidate(context)

    val solution1 = Try(solver1.tryPlan(queryGraph))
    val solution2 = Try(solver2.tryPlan(queryGraph))

    val availableSolutions = (solution1, solution2) match {
      case (Success(s1), Success(s2)) => s1.toSeq ++ s2.toSeq
      case (Failure(_:HintException), Success(s2)) => s2.toSeq
      case (Success(s1), Failure(_:HintException)) => s1.toSeq
      case (Failure(e), _) => throw e
      case (_, Failure(e)) => throw e
    }

    pickBest(availableSolutions)
  }
}
