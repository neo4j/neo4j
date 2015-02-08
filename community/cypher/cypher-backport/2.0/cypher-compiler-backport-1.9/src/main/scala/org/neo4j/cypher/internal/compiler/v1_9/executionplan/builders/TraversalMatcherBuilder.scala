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
package org.neo4j.cypher.internal.compiler.v1_9.executionplan.builders

import org.neo4j.cypher.internal.compiler.v1_9.executionplan.PlanBuilder
import org.neo4j.cypher.internal.compiler.v1_9.commands._
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.graphdb
import graphdb.{Node, GraphDatabaseService}
import org.neo4j.cypher.internal.compiler.v1_9.pipes.{ParameterPipe, TraversalMatchPipe, EntityProducer}
import org.neo4j.cypher.internal.compiler.v1_9.pipes.matching.{Trail, TraversalMatcher, MonoDirectionalTraversalMatcher, BidirectionalTraversalMatcher}
import org.neo4j.cypher.internal.compiler.v1_9.executionplan.ExecutionPlanInProgress
import org.neo4j.cypher.internal.compiler.v1_9.commands.NodeByIndex
import org.neo4j.cypher.internal.compiler.v1_9.commands.NodeByIndexQuery

class TraversalMatcherBuilder(graph: GraphDatabaseService) extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress): ExecutionPlanInProgress = extractExpanderStepsFromQuery(plan) match {
    case None              => throw new ThisShouldNotHappenError("Andres", "This plan should not have been accepted")
    case Some(longestPath) =>
      val LongestTrail(start, end, longestTrail) = longestPath

      val unsolvedItems = plan.query.start.filter(_.unsolved)
      val (startToken, startNodeFn) = identifier2nodeFn(graph, start, unsolvedItems)

      val (matcher,tokens) = chooseCorrectMatcher(end, longestPath, startNodeFn, startToken, unsolvedItems)

      val solvedPatterns = longestTrail.patterns

      val newWhereClause = markPredicatesAsSolved(plan, longestTrail)

      val newQ = plan.query.copy(
        patterns = plan.query.patterns.filterNot(p => solvedPatterns.contains(p.token)) ++ solvedPatterns.map(Solved(_)),
        start = markStartItemsSolved(plan.query.start, tokens, longestTrail),
        where = newWhereClause
      )

      val pipe = new TraversalMatchPipe(plan.pipe, matcher, longestTrail)

      plan.copy(pipe = pipe, query = newQ)
  }

  private def markStartItemsSolved(startItems: Seq[QueryToken[StartItem]], done: Seq[QueryToken[StartItem]], trail:Trail): Seq[QueryToken[StartItem]] = {
    val newStart = startItems.filterNot(done.contains) ++ done.map(_.solve)

    newStart.map {
      case t@Unsolved(AllNodes(key)) if key == trail.end => t.solve
      case x                                             => x
    }
  }

  private def markPredicatesAsSolved(in: ExecutionPlanInProgress, trail: Trail): Seq[QueryToken[Predicate]] = {
    val originalWhere = in.query.where
    val predicates = trail.predicates.toList.filterNot(predicate => {
      val symbolsNeeded = predicate.symbolTableDependencies
      symbolsNeeded.contains(trail.start) || symbolsNeeded.contains(trail.end) // The traversal matcher can't handle
                                                                               // predicates at the ends
    })
    val (solvedPreds, old) = originalWhere.partition(pred => predicates.contains(pred.token))

    old ++ solvedPreds.map(_.solve)
  }

  private def chooseCorrectMatcher(end:Option[String],
                           longestPath:LongestTrail,
                           startNodeFn:EntityProducer[Node],
                           startToken:QueryToken[StartItem],
                           unsolvedItems: Seq[QueryToken[StartItem]] ): (TraversalMatcher,Seq[QueryToken[StartItem]]) = {
    val (matcher, tokens) = if (end.isEmpty) {
      val matcher = new MonoDirectionalTraversalMatcher(longestPath.step, startNodeFn)
      (matcher, Seq(startToken))
    } else {
      val (endToken, endNodeFn) = identifier2nodeFn(graph, end.get, unsolvedItems)
      val step = longestPath.step
      val matcher = new BidirectionalTraversalMatcher(step, startNodeFn, endNodeFn)
      (matcher, Seq(startToken, endToken))
    }
    (matcher,tokens)
  }

  def identifier2nodeFn(graph: GraphDatabaseService, identifier: String, unsolvedItems: Seq[QueryToken[StartItem]]):
  (QueryToken[StartItem], EntityProducer[Node]) = {
    val token = unsolvedItems.filter { (item) => identifier == item.token.identifierName }.head
    (token, IndexQueryBuilder.getNodeGetter(token.token, graph))
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = {
    val steps = extractExpanderStepsFromQuery(plan)
    steps.nonEmpty && plan.pipe.isInstanceOf[ParameterPipe]
  }

  private def extractExpanderStepsFromQuery(plan: ExecutionPlanInProgress): Option[LongestTrail] = {
    val startPoints = plan.query.start.flatMap {
      case Unsolved(NodeByIndexQuery(id, _, _)) => Some(id)
      case Unsolved(NodeByIndex(id, _, _, _))   => Some(id)
      case Unsolved(NodeById(id, _))            => Some(id)
      case _                                    => None
    }

    val pattern = plan.query.patterns.flatMap {
      case Unsolved(r: RelatedTo) if !r.optional && r.left != r.right         => Some(r)
      case Unsolved(r: VarLengthRelatedTo) if !r.optional && r.start != r.end => Some(r)
      case _                                                                  => None
    }

    val preds = plan.query.where.filter(_.unsolved).map(_.token).
    // TODO We should not filter these out. This should be removed once we only use TraversalMatcher
    filterNot {
      case pred => pred.exists( exp => exp.isInstanceOf[PathExpression] )
    }

    TrailBuilder.findLongestTrail(pattern, startPoints, preds)
  }

  def priority = PlanBuilder.TraversalMatcher
}

