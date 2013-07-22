/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.executionplan.{PartiallySolvedQuery, PlanBuilder, ExecutionPlanInProgress}
import org.neo4j.cypher.internal.spi.PlanContext
import org.neo4j.cypher.internal.commands._
import org.neo4j.cypher.internal.mutation.{UpdateAction, MergeNodeAction}
import org.neo4j.graphdb.Node
import org.neo4j.cypher.internal.pipes.EntityProducer

/*
This builder is concerned with finding queries without start items and without index hints, and
choosing a start point to use.

To do this, three things are done.
  * every disconnected part of the pattern is found
  * for each pattern:
    * find the best strategy for retrieving nodes for every pattern node
    * for each pattern, find the best start points
 */


//TODO: We should mark predicates used to find start points as solved
class StartPointChoosingBuilder extends PlanBuilder {
  val Single = 0

  type LabelName = String
  type IdentifierName = String
  type PropertyKey = String

  val entityProducerFactory = new EntityProducerFactory


  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext): ExecutionPlanInProgress = {

    val q: PartiallySolvedQuery = plan.query

    // Find disconnected patterns, and make sure we have start points for all of them
    val disconnectedStarItems: Seq[QueryToken[StartItem]] = findStartItemsForDisconnectedPatterns(plan, ctx).map(Unsolved(_))

    // Find merge points that do not have a node producer, and produce one for them
    val updatesWithSolvedMergePoints = plan.query.updates.map(solveUnsolvedMergePoints(ctx))

    plan.copy(query = q.copy(start = disconnectedStarItems ++ q.start, updates = updatesWithSolvedMergePoints))
  }

  private def solveUnsolvedMergePoints(ctx: PlanContext): (QueryToken[UpdateAction] => QueryToken[UpdateAction]) = {
    case Unsolved(mergeNodeAction@MergeNodeAction(identifier, where, _, _, None)) =>
      val startItem = NodeFetchStrategy.findStartStrategy(identifier, where, ctx)
      val nodeProducer: EntityProducer[Node] = entityProducerFactory.nodeStartItems(ctx, startItem.s)
      val predicatesLeft = where.toSet -- startItem.solvedPredicates

      val newMergeNodeAction = mergeNodeAction.copy(
        nodeProducerOption = Some(nodeProducer),
        expectations = predicatesLeft.toSeq)

      Unsolved(newMergeNodeAction)
    case x                                                                                            => x

  }

  private def findStartItemsForDisconnectedPatterns(plan: ExecutionPlanInProgress, ctx: PlanContext): Seq[StartItem] = {
    val disconnectedPatterns = plan.query.matchPattern.disconnectedPatternsWithout(plan.pipe.symbols.keys)
    val startPointNames = plan.query.start.map(_.token.identifierName)
    val allPredicates = plan.query.where.map(_.token)

    def findSingleNodePoints(startPoints: Set[RatedStartItem]): Iterable[StartItem] =
      startPoints.collect {
        case RatedStartItem(si, r, _) if r == Single => si
      }

    def findStartItemFor(pattern: MatchPattern): Iterable[StartItem] = {
      val shortestPathPoints: Set[IdentifierName] = plan.query.patterns.collect {
        case Unsolved(ShortestPath(_, start, end, _, _, _, _, _, _)) => Seq(start, end)
      }.flatten.toSet

      val startPoints: Set[RatedStartItem] =
        pattern.nodes.map(key => NodeFetchStrategy.findStartStrategy(key, allPredicates, ctx)).toSet

      val singleNodePoints = findSingleNodePoints(startPoints)
      val shortestPathPointsInPattern: Set[IdentifierName] = shortestPathPoints intersect pattern.nodes.toSet

      if (shortestPathPointsInPattern.nonEmpty) {
        startPoints.collect {
          case RatedStartItem(si, r, _) if shortestPathPoints.contains(si.identifierName) => si
        }.toSet union singleNodePoints.toSet
      } else if (singleNodePoints.nonEmpty) {
        // We want to keep all these start points because cartesian product with them is free
        singleNodePoints
      } else {
        // Lastly, let's pick the best start point possible
        Some(startPoints.toSeq.sortBy(_.rating).head.s)
      }
    }

    disconnectedPatterns.flatMap(
      (pattern: MatchPattern) => {
        val startPointsAlreadyInPattern = (startPointNames intersect pattern.nodes)

        if (startPointsAlreadyInPattern.isEmpty)
          findStartItemFor(pattern)
        else
          None
      }
    )
  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext) =
    !plan.query.extracted && plan != apply(plan, ctx) // TODO: This can be optimized

  def priority = PlanBuilder.IndexLookup
}