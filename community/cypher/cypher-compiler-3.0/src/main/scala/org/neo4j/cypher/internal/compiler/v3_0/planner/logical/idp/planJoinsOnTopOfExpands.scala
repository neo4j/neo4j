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
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.{LogicalPlanningSupport, LogicalPlanningContext}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.LogicalPlan

object planJoinsOnTopOfExpands {

  import LogicalPlanningSupport._

  def apply(qg: QueryGraph, lhsOpt: Option[LogicalPlan], rhsSet: Set[LogicalPlan])
           (implicit context: LogicalPlanningContext): Iterator[LogicalPlan] = {

    val planProducer = context.logicalPlanProducer
    val result = for (
      lhs <- lhsOpt.iterator;
      joinHints = qg.joinHints if joinHints.nonEmpty;
      rhs <- rhsSet;
      overlap = lhs.solved.queryGraph.patternNodes intersect rhs.solved.queryGraph.patternNodes if overlap.nonEmpty
    ) yield {
        val matchingHints = joinHints.filter(_.coveredBy(overlap)).toSet
        Iterator(
          planProducer.planNodeHashJoin(overlap, lhs, rhs, matchingHints),
          planProducer.planNodeHashJoin(overlap, rhs, lhs, matchingHints)
        )
      }

    result.flatten
  }
}
