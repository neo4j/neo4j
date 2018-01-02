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

import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.Predicate
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{MatchPattern, PartiallySolvedQuery, PlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.ExecutionPlanInProgress
import org.neo4j.cypher.internal.compiler.v2_3.commands.ShortestPath
import org.neo4j.cypher.internal.compiler.v2_3.pipes.PipeMonitor

/*
This builder is concerned with finding queries without start items and without index hints, and
choosing a start point to use.

To do this, three things are done.
  * every disconnected part of the pattern is found
  * for each pattern:
    * find the best strategy for retrieving nodes for every pattern node
    * for each pattern, find the best start points
 */


class StartPointChoosingBuilder extends PlanBuilder {
  type LabelName = String
  type IdentifierName = String
  type PropertyKey = String

  val entityProducerFactory = new EntityProducerFactory


  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {

    val q: PartiallySolvedQuery = plan.query

    // Find disconnected patterns, and make sure we have start points for all of them
    val disconnectedRatedStartItems: Seq[RatedStartItem] = findStartItemsForDisconnectedPatterns(plan, ctx)

    val solvedPredicates: Seq[Predicate] = disconnectedRatedStartItems.flatMap(_.solvedPredicates)
    val newUnsolvedPredicates: Seq[QueryToken[Predicate]] = disconnectedRatedStartItems.flatMap(_.newUnsolvedPredicates).map(Unsolved(_))
    val disconnectedStartItems = disconnectedRatedStartItems.map( (r: RatedStartItem) => Unsolved(r.s) )

    val filteredWhere = solvedPredicates.foldLeft(q.where) {
      (currentWhere: Seq[QueryToken[Predicate]], predicate: Predicate) =>
        currentWhere.replace(Unsolved(predicate), Solved(predicate))
    }

    val amendedWhere = filteredWhere ++ newUnsolvedPredicates

    plan.copy(query = q.copy(start = disconnectedStartItems ++ q.start, where = amendedWhere))
  }

  private def findStartItemsForDisconnectedPatterns(plan: ExecutionPlanInProgress, ctx: PlanContext): Seq[RatedStartItem] = {
    val disconnectedPatterns = plan.query.matchPattern.disconnectedPatternsWithout(plan.pipe.symbols.keys)
    val startPointNames = plan.query.start.map(_.token.identifierName)
    val allPredicates = plan.query.where.map(_.token)

    def findSingleNodePoints(startPoints: Set[RatedStartItem]): Iterable[RatedStartItem] =
      startPoints.filter {
        case RatedStartItem(si, r, _, _) => r == NodeFetchStrategy.Single
      }

    def findStartItemFor(pattern: MatchPattern): Iterable[RatedStartItem] = {
      val shortestPathPoints: Set[IdentifierName] = plan.query.patterns.collect {
        case Unsolved(ShortestPath(_, start, end, _, _, _, _, _, _)) => Seq(start.name, end.name)
      }.flatten.toSet

      val startPoints: Set[RatedStartItem] =
        pattern.nodes.map(key => NodeFetchStrategy.findStartStrategy(key, allPredicates, ctx, plan.pipe.symbols)).toSet

      val singleNodePoints = findSingleNodePoints(startPoints)
      val shortestPathPointsInPattern: Set[IdentifierName] = shortestPathPoints intersect pattern.nodes.toSet

      if (shortestPathPointsInPattern.nonEmpty) {
        startPoints.filter {
          case RatedStartItem(si, r, _, _) => shortestPathPoints.contains(si.identifierName)
        }.toSet union singleNodePoints.toSet
      } else if (singleNodePoints.nonEmpty) {
        // We want to keep all these start points because cartesian product with them is free
        singleNodePoints
      } else {
        // Lastly, let's pick the best start point possible
        Some(startPoints.toSeq.sortBy(_.rating).head)
      }
    }

    disconnectedPatterns.flatMap(
      (pattern: MatchPattern) => {
        val patternElements = pattern.nodes ++ pattern.relationships.flatMap(_.name)

        val startPointsAlreadyInPattern = startPointNames intersect patternElements

        if (startPointsAlreadyInPattern.isEmpty)
          findStartItemFor(pattern)
        else
          None
      }
    )
  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) =
    !plan.query.extracted && plan != apply(plan, ctx) // TODO: This can be optimized
}
