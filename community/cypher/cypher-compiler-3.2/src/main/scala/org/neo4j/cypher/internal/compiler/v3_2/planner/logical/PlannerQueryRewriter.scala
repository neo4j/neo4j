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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical

import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v3_2.planner.{AggregatingQueryProjection, RegularPlannerQuery}
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, topDown}

case object PlannerQueryRewriter extends Rewriter {

  private val instance: Rewriter = topDown(Rewriter.lift {
    case RegularPlannerQuery(graph, proj: AggregatingQueryProjection, tail) if proj.aggregationExpressions.isEmpty =>
      val distinctExpressions = proj.groupingKeys

      // The variables that are needed by the return clause
      val variableDeps = distinctExpressions.values.flatMap(_.dependencies).map(IdName.fromVariable).toSet

      val optionalMatches = graph.optionalMatches.flatMap { optionalGraph =>
        val mustKeep = graph.smallestGraphIncluding(variableDeps -- optionalGraph.argumentIds)
        if (mustKeep.isEmpty)
          None
        else
          Some(optionalGraph)
      }

      val projection = AggregatingQueryProjection(distinctExpressions, Map.empty, proj.shuffle)
      val matches = graph.withOptionalMatches(optionalMatches)
      RegularPlannerQuery(matches, horizon = projection, tail = tail)
  })

  override def apply(input: AnyRef) = instance.apply(input)

}
