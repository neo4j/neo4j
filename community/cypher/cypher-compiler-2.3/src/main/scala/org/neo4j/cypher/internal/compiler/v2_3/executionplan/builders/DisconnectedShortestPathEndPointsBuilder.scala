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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_3.commands.ShortestPath
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.Predicate
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.PipeMonitor
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext


class DisconnectedShortestPathEndPointsBuilder extends PlanBuilder {
  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = unsolvedEndPoints(plan).nonEmpty

  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor): ExecutionPlanInProgress = {
    val nodesToAdd = unsolvedEndPoints(plan)
    val allPredicates = plan.query.where.map(_.token)

    val startPoints: Set[RatedStartItem] =
      nodesToAdd.map(key => NodeFetchStrategy.findStartStrategy(key, allPredicates, ctx, plan.pipe.symbols)).toSet

    val singleNodeToAdd: RatedStartItem = startPoints.toSeq.sortBy(_.rating).head

    val filteredWhere = singleNodeToAdd.solvedPredicates.foldLeft(plan.query.where) {
      (currentWhere: Seq[QueryToken[Predicate]], predicate: Predicate) =>
        currentWhere.replace(Unsolved(predicate), Solved(predicate))
    }

    val amendedWhere = filteredWhere ++ singleNodeToAdd.newUnsolvedPredicates.map(Unsolved(_))

    plan.copy(query = plan.query.copy(start = plan.query.start :+ Unsolved(singleNodeToAdd.s), where = amendedWhere))
  }

  private def unsolvedEndPoints(plan: ExecutionPlanInProgress): Set[String] = {
    val shortestPathEndPoints: Set[String] = plan.query.patterns.collect {
      case Unsolved(ShortestPath(_, start, end, _, _, _, _, _, _)) => Seq(start.name, end.name)
    }.flatten.toSet

    shortestPathEndPoints.filter {
      case p =>
        !plan.pipe.symbols.hasIdentifierNamed(p) &&
        !plan.query.start.exists(startItem => p == startItem.token.identifierName)
    }
  }
}
