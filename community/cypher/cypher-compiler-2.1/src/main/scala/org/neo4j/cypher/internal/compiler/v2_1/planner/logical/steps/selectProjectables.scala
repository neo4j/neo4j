/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{LogicalPlanContext, PlanTransformer}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.LogicalPlan

object selectProjectables extends PlanTransformer {
  def apply(plan: LogicalPlan)(implicit context: LogicalPlanContext): LogicalPlan = {

    /*
      TODO: Possible improvement to consider when doing projections (WITH, RETURN)

      plan =>
      select[P0]( plan ) =>
      fake_project( select ( plan ) ) =>
      select[P1]( fake_project( select ( plan ) ) )

      P0: Predicates after initial selection
      P1: Predicates when selecting under the assumption that everything has been projected

      List of all projections N = { n1, n2, n3 }
      List of predicates that may be solved when all has been projected D = P1 -- P0 = { p1(...), .., pk(...) }

      Sort D by selectivity

      Build plan such that we select according to this order (trim down cardinality early)
    */
    val selectedPlan = selectCovered(plan)

    val allNamedPathPlan = projectNamedPaths(_ => true)(selectedPlan)
    val allNamedPathPlanWithSelections = selectCovered(allNamedPathPlan)

    val selectedNamedPathIds = allNamedPathPlanWithSelections.coveredIds -- selectedPlan.coveredIds

    val namedPathPlan = projectNamedPaths( (namedPath) => selectedNamedPathIds.contains(namedPath.name) )(selectedPlan)

    if (namedPathPlan eq selectedPlan)
      namedPathPlan
    else
      selectCovered( namedPathPlan )
  }
}
