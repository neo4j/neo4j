/**
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.pickBestPlan

class CompositeQueryGraphSolver(solver1: TentativeQueryGraphSolver, solver2: TentativeQueryGraphSolver)
  extends TentativeQueryGraphSolver {

  // NOTE: we assume that the PlanTable type is the same between the 2 solvers
  def emptyPlanTable: PlanTable = solver1.emptyPlanTable

  def tryPlan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]) = {
    val availableSolutions = solver1.tryPlan(queryGraph).toSeq ++ solver2.tryPlan(queryGraph)
    pickBestPlan(availableSolutions.iterator)
  }
}
