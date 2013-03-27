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

import org.neo4j.cypher.internal.executionplan.{PlanBuilder, ExecutionPlanInProgress}
import org.neo4j.cypher.internal.commands._
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.graphdb
import org.neo4j.cypher.internal.pipes.NullPipe
import graphdb.Node
import org.neo4j.cypher.internal.pipes.{TraversalMatchPipe, EntityProducer}
import org.neo4j.cypher.internal.pipes.matching.{Trail, TraversalMatcher, MonoDirectionalTraversalMatcher, BidirectionalTraversalMatcher}
import org.neo4j.cypher.internal.commands.NodeByIndex
import org.neo4j.cypher.internal.commands.NodeByIndexQuery
import org.neo4j.cypher.internal.symbols.{NodeType, SymbolTable}
import org.neo4j.cypher.internal.spi.PlanContext

class TraversalMatcherBuilder extends PlanBuilder with PatternGraphBuilder {
  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext): ExecutionPlanInProgress =
    extractExpanderStepsFromQuery(plan) match {
      case None              => throw new ThisShouldNotHappenError("Andres", "This plan should not have been accepted")
      case Some(longestPath) =>
        val LongestTrail(start, end, longestTrail) = longestPath

        val unsolvedItems = plan.query.start.filter(_.unsolved)
        val (startToken, startNodeFn) = identifier2nodeFn(ctx, start, unsolvedItems)

        val (matcher, tokens) = chooseCorrectMatcher(end, longestPath, startNodeFn, startToken, unsolvedItems, ctx)

        val solvedPatterns = longestTrail.patterns

        checkPattern(plan, tokens)

        val newWhereClause = markPredicatesAsSolved(plan, longestTrail)

        val newQ = plan.query.copy(
          patterns = plan.query.patterns.filterNot(p => solvedPatterns.contains(p.token)) ++ solvedPatterns.map(Solved(_)),
          start = plan.query.start.filterNot(tokens.contains) ++ tokens.map(_.solve),
          where = newWhereClause
        )

        val pipe = new TraversalMatchPipe(plan.pipe, matcher, longestTrail)

        plan.copy(pipe = pipe, query = newQ)
    }

  private def checkPattern(plan: ExecutionPlanInProgress, tokens: Seq[QueryToken[StartItem]]) {
    val newIdentifiers = tokens.map(_.token).map(x => x.identifierName -> NodeType()).toMap
    val newSymbolTable = plan.pipe.symbols.add(newIdentifiers)
    validatePattern(newSymbolTable, plan.query.patterns.map(_.token))
  }


  private def validatePattern(symbols: SymbolTable, patterns: Seq[Pattern]) = {
    //We build the graph here, because the pattern graph builder finds problems with the pattern
    //that we don't find other wise. This should be moved out from the patternGraphBuilder, but not right now
    buildPatternGraph(symbols, patterns)
  }


  private def markPredicatesAsSolved(in: ExecutionPlanInProgress, trail: Trail): Seq[QueryToken[Predicate]] = {
    val originalWhere = in.query.where

    // Let's not look at the predicates for the outer most part of the trail
    val predicates = trail.predicates.drop(1).dropRight(1).flatten.distinct

    val (solvedPreds, old) = originalWhere.partition(pred => predicates.contains(pred.token))

    old ++ solvedPreds.map(_.solve)
  }

  private def chooseCorrectMatcher(end:Option[String],
                           longestPath:LongestTrail,
                           startNodeFn:EntityProducer[Node],
                           startToken:QueryToken[StartItem],
                           unsolvedItems: Seq[QueryToken[StartItem]],
                           ctx:PlanContext): (TraversalMatcher,Seq[QueryToken[StartItem]]) = {
    val (matcher, tokens) = if (end.isEmpty) {
      val matcher = new MonoDirectionalTraversalMatcher(longestPath.step, startNodeFn)
      (matcher, Seq(startToken))
    } else {
      val (endToken, endNodeFn) = identifier2nodeFn(ctx, end.get, unsolvedItems)
      val step = longestPath.step
      val matcher = new BidirectionalTraversalMatcher(step, startNodeFn, endNodeFn)
      (matcher, Seq(startToken, endToken))
    }
    (matcher,tokens)
  }

  def identifier2nodeFn(ctx:PlanContext, identifier: String, unsolvedItems: Seq[QueryToken[StartItem]]):
  (QueryToken[StartItem], EntityProducer[Node]) = {
    val startItemQueryToken = unsolvedItems.filter { (item) => identifier == item.token.identifierName }.head
    (startItemQueryToken, mapNodeStartCreator()(ctx, startItemQueryToken.token))
  }

  private def mapNodeStartCreator(): PartialFunction[(PlanContext, StartItem), EntityProducer[Node]] = {
    val entityFactory = new EntityProducerFactory

    entityFactory.nodeById orElse
    entityFactory.nodeByIndex orElse
    entityFactory.nodeByIndexQuery orElse
    entityFactory.nodeByIndexHint
  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext): Boolean = {
    val steps = extractExpanderStepsFromQuery(plan)
    steps.nonEmpty && plan.pipe == NullPipe
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
      case pred => pred.exists( exp => exp.isInstanceOf[PatternPredicate] )
    }

    TrailBuilder.findLongestTrail(pattern, startPoints, preds)
  }

  def priority = PlanBuilder.TraversalMatcher
}

