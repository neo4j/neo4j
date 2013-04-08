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
import org.neo4j.cypher.internal.commands.expressions.{Property, Identifier}
import org.neo4j.cypher.internal.commands.SchemaIndex
import org.neo4j.cypher.internal.commands.HasLabel
import org.neo4j.cypher.internal.commands.Equals
import org.neo4j.cypher.internal.commands.NodeByLabel

/*
This builder is concerned with finding queries without start items and without index hints, and
choosing a start point to use.

To do this, three things are done.
  * every disconnected part of the pattern is found
  * for each pattern:
    * find the best strategy for retreiving nodes for every pattern node
    * for each pattern, find the best start points
 */

class StartPointChoosingBuilder extends PlanBuilder {
  val Single = 0
  val IndexEquality = 1
  val IndexRange = 1
  val IndexScan = 2
  val LabelScan = 3
  val Global = 4

  type LabelName = String
  type IdentifierName = String
  type PropertyKey = String

  case class StartItemWithRating(s: StartItem, rating: Integer)

  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext): ExecutionPlanInProgress = {
    val q: PartiallySolvedQuery = plan.query
    val newQuery =
      q.start match {
        case Seq() if q.matchPattern.nonEmpty => q.copy(start = findStartItemsForEachPattern(plan, ctx).map(Unsolved(_)))
        case _                                => q
      }


    plan.copy(query = newQuery)
  }


  private def findStartItemsForEachPattern(plan: ExecutionPlanInProgress, ctx: PlanContext): Seq[StartItem] = {
    val patterns = plan.query.matchPattern.disconnectedPatternsWithout(plan.pipe.symbols.keys)
    val shortestPathPoints: Set[IdentifierName] = plan.query.patterns.collect {
      case Unsolved(ShortestPath(_, start, end, _,_,_,_,_,_)) => Seq(start, end)
    }.flatten.toSet

    def findSingleNodePoints(startPoints: Set[StartItemWithRating]): Iterable[StartItem] =
      startPoints.collect { case StartItemWithRating(si, r) if r == Single => si }


    patterns.flatMap(
      (pattern: MatchPattern) => {
        val startPoints: Set[StartItemWithRating] =
          pattern.nodes.map(key => findStartStrategy(key, pattern, plan.query.where, ctx)).toSet

        val singleNodePoints = findSingleNodePoints(startPoints)
        val shortestPathPointsInPattern: Set[IdentifierName] = shortestPathPoints intersect pattern.nodes.toSet

        if (shortestPathPointsInPattern.nonEmpty) {
          startPoints.collect {
            case StartItemWithRating(si, r) if shortestPathPoints.contains(si.identifierName) => si
          }.toSet union singleNodePoints.toSet
        } else if (singleNodePoints.nonEmpty) {
          // We want to keep all these start points because cartesian product with them is free
          singleNodePoints
        } else {
          // Lastly, let's pick the best start point possible
          Some(startPoints.toSeq.sortBy(_.rating).head.s)
        }
      }
    )
  }

  private def findStartStrategy(identifier: String, plan: MatchPattern, where: Seq[QueryToken[Predicate]], ctx: PlanContext):
  StartItemWithRating = {
    val labels: Seq[LabelName] = findLabelsForNode(identifier, plan, where)
    val propertyPredicates: Seq[PropertyKey] = findEqualityPredicatesOnProperty(identifier, where)

    val indexSeeks = for (
      label <- labels;
      property <- propertyPredicates
      if (ctx.getIndexRuleId(label, property).nonEmpty)
    ) yield SchemaIndex(identifier, label, property, None)

    if (indexSeeks.nonEmpty) {
      // TODO: Once we have index statistics, we can pick the best one
      StartItemWithRating(indexSeeks.head, IndexEquality)
    } else if (labels.nonEmpty) {
      // TODO: Once we have label statistics, we can pick the best one
      StartItemWithRating(NodeByLabel(identifier, labels.head), LabelScan)
    } else {
      StartItemWithRating(AllNodes(identifier), Global)
    }
  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext) = !plan.query.extracted && plan != apply(plan, ctx) // TODO: This can be optimized

  def priority = PlanBuilder.IndexLookup

  private def findLabelsForNode(node: String, pattern: MatchPattern, where: Seq[QueryToken[Predicate]]): Seq[LabelName] =
    where.collect {
      case Unsolved(HasLabel(Identifier(identifier), labelNames)) if identifier == node => labelNames
    }.
      flatten.
      map(_.name)

  private def findEqualityPredicatesOnProperty(identifier: IdentifierName, where: Seq[QueryToken[Predicate]]): Seq[PropertyKey] =
    where.collect {
      case Unsolved(Equals(Property(Identifier(id), propertyName), expression)) if id == identifier => propertyName
      case Unsolved(Equals(expression, Property(Identifier(id), propertyName))) if id == identifier => propertyName
    }
}


