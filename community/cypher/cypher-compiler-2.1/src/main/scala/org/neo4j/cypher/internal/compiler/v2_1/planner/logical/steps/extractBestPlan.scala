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

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.LogicalPlanContext
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.PlanTable
import org.neo4j.cypher.internal.compiler.v2_1.planner.CantHandleQueryException
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{LogicalPlan, SingleRow}

object extractBestPlan {
  def apply(planTable: PlanTable)(implicit context: LogicalPlanContext): LogicalPlan = {
    if (planTable.size > 1)
      throw new InternalException(s"Expected the final plan table to have 0 or 1 plan (got ${planTable.size})")

    val bestPlan = planTable.plans.headOption.getOrElse(SingleRow())
    if (!context.queryGraph.selections.coveredBy(bestPlan.solvedPredicates))
      throw new CantHandleQueryException

    val remainingPatterns = context.queryGraph.patternRelationships -- bestPlan.solvedPatterns.toSet
    if (remainingPatterns.nonEmpty)
      throw new CantHandleQueryException

    bestPlan
  }
}
