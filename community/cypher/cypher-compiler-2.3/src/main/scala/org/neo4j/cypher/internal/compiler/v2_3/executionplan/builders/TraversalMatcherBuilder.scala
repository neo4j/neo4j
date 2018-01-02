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

import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{IdFunction, Identifier}
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{Equals, Predicate}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching.{Trail, TraversalMatcher}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{EntityProducer, PipeMonitor, SingleRowPipe, TraversalMatchPipe}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.Node
import org.neo4j.helpers.ThisShouldNotHappenError

class TraversalMatcherBuilder extends PlanBuilder with PatternGraphBuilder {
  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor): ExecutionPlanInProgress =
    extractExpanderStepsFromQuery(plan) match {
      case (None,_) => throw new ThisShouldNotHappenError("Andres", "This plan should not have been accepted")
      case (Some(longestPath), solvedStartItems) =>
        val LongestTrail(start, end, longestTrail) = longestPath

        val unsolvedItems = plan.query.start.filter(_.unsolved)
        val (startToken, startNodeFn) = identifier2nodeFn(ctx, start, unsolvedItems)

        val (matcher, tokens) = chooseCorrectMatcher(end, longestPath, startNodeFn, startToken, unsolvedItems, ctx)

        val solvedPatterns = longestTrail.patterns

        checkPattern(plan, tokens)

        val newWhereClause = markPredicatesAsSolved(plan, longestTrail)

        val newQ = plan.query.copy(
          patterns = plan.query.patterns.filterNot(p => solvedPatterns.contains(p.token)) ++ solvedPatterns.map(Solved(_)),
          start = markStartItemsSolved(plan.query.start, tokens ++ solvedStartItems, longestTrail),
          where = newWhereClause
        )

        val pipe = new TraversalMatchPipe(plan.pipe, matcher, longestTrail)

        plan.copy(pipe = pipe, query = newQ)
    }

  private def checkPattern(plan: ExecutionPlanInProgress, tokens: Seq[QueryToken[StartItem]]) {
    val newIdentifiers = tokens.map(_.token).map(x => x.identifierName -> CTNode).toMap
    val newSymbolTable = plan.pipe.symbols.add(newIdentifiers)
    validatePattern(newSymbolTable, plan.query.patterns.map(_.token))
  }


  private def validatePattern(symbols: SymbolTable, patterns: Seq[Pattern]) = {
    //We build the graph here, because the pattern graph builder finds problems with the pattern
    //that we don't find other wise. This should be moved out from the patternGraphBuilder, but not right now
    buildPatternGraph(symbols, patterns)
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

    val predicates = trail.predicates.flatten.filterNot(predicate => {
      val symbolsNeeded = predicate.symbolTableDependencies
      symbolsNeeded.contains(trail.start) || symbolsNeeded.contains(trail.end) // The traversal matcher can't handle
                                                                               // predicates at the ends
    })
    val (solvedPreds, old) = originalWhere.partition(pred => predicates.contains(pred.token))

    old ++ solvedPreds.map(_.solve)
  }

  private def chooseCorrectMatcher(end: Option[String],
                                   longestPath: LongestTrail,
                                   startNodeFn: EntityProducer[Node],
                                   startToken: QueryToken[StartItem],
                                   unsolvedItems: Seq[QueryToken[StartItem]],
                                   ctx: PlanContext): (TraversalMatcher, Seq[QueryToken[StartItem]]) = {
    val (matcher, tokens) = if (end.isEmpty) {
      val matcher = ctx.monoDirectionalTraversalMatcher(longestPath.step, startNodeFn)
      (matcher, Seq(startToken))
    } else {
      val (endToken, endNodeFn) = identifier2nodeFn(ctx, end.get, unsolvedItems)
      val step = longestPath.step
      val matcher = ctx.bidirectionalTraversalMatcher(step, startNodeFn, endNodeFn)
      (matcher, Seq(startToken, endToken))
    }
    (matcher, tokens)
  }

  def identifier2nodeFn(ctx:PlanContext, identifier: String, unsolvedItems: Seq[QueryToken[StartItem]]):
  (QueryToken[StartItem], EntityProducer[Node]) = {
    val startItemQueryToken = unsolvedItems.filter { (item) => identifier == item.token.identifierName }.head
    (startItemQueryToken, mapNodeStartCreator()((ctx, startItemQueryToken.token)))
  }

  val entityFactory = new EntityProducerFactory

  private def mapNodeStartCreator(): PartialFunction[(PlanContext, StartItem), EntityProducer[Node]] =
    entityFactory.readNodeStartItems

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor): Boolean = {
    val (longest,_) = extractExpanderStepsFromQuery(plan)
      plan.pipe.isInstanceOf[SingleRowPipe] &&
      !plan.query.optional &&
      longest.exists(doesNotOverlapExistingSymbols(plan.pipe.symbols))
  }

  private def doesNotOverlapExistingSymbols(symbols: SymbolTable)(trail: LongestTrail) =
    (trail.longestTrail.symbols(new SymbolTable()).keys intersect symbols.keys).isEmpty

  private def extractExpanderStepsFromQuery(plan: ExecutionPlanInProgress): (Option[LongestTrail], Seq[QueryToken[StartItem]]) = {
    val startPoints = plan.query.start.flatMap {
      case Unsolved(x: NodeStartItemIdentifiers) => Some(x.identifierName)
      case _            => None
    }

    val pattern = plan.query.patterns.flatMap {
      case Unsolved(r: RelatedTo) if r.left != r.right          => Some(r)
      case Unsolved(r: VarLengthRelatedTo) if r.left != r.right => Some(r)
      case _                                                    => None
    }

    val unsolvedRelNames: Seq[String] = plan.query.patterns.collect {
      case Unsolved(r: RelatedTo) if r.left != r.right => r.relName
    }

    val relIdPreds = plan.query.start.collect[(QueryToken[StartItem], AnyInCollection), Seq[(QueryToken[StartItem], AnyInCollection)]] {
      case qt@Unsolved(x: RelationshipById) if unsolvedRelNames.contains(x.identifierName) =>
        val compKey: String = s"--relId-${x.identifierName}--"
        (qt, AnyInCollection(
          x.expression, compKey,
          Equals(IdFunction(Identifier(x.identifierName)),
          Identifier(compKey))))
    }

    val preds = plan.query.where.filter(_.unsolved).map(_.token).
    // TODO We should not filter these out. This should be removed once we only use TraversalMatcher
    filterNot {
      case pred => pred.exists( exp => exp.isInstanceOf[PathExpression]  )
    }

    (TrailBuilder.findLongestTrail(pattern, startPoints, preds ++ relIdPreds.map(_._2)), relIdPreds.map(_._1))
  }
}

