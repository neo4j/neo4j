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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_0.executionplan.{Phase, PartiallySolvedQuery, PlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_0.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_0.mutation._
import org.neo4j.cypher.internal.compiler.v2_0.pipes._
import org.neo4j.cypher.internal.compiler.v2_0.commands.{Pattern, Query}
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Identifier
import org.neo4j.cypher.internal.compiler.v2_0.symbols.{CypherType, NodeType, SymbolTable}
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.ExecutionPlanInProgress
import org.neo4j.cypher.internal.compiler.v2_0.commands.AllIdentifiers
import org.neo4j.cypher.InternalException

/*
This class solves MERGE for patterns. It does this by creating an execution plan that uses normal pattern matching
to find matches for a pattern. If that step returns nothing, the missing parts of the pattern are created.

By doing it this way, we rely on already existing code to both match and create the elements.

This class prepares MergePatternAction objects to be run by creating the match pipe
*/
case class MergePatternBuilder(matching: Phase) extends PlanBuilder {
  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext): Boolean =
    plan.query.updates.exists {
      case Unsolved(x: MergePatternAction)  => !x.readyToExecute && x.symbolDependenciesMet(plan.pipe.symbols)
      case Unsolved(foreach: ForeachAction) => interesting(foreach, plan.pipe.symbols)
      case _                                => false
    }

  def apply(in: ExecutionPlanInProgress, ctx: PlanContext): ExecutionPlanInProgress = {

    extractFrom(in.query.updates, in.pipe.symbols) match {
      case originalMerge: MergePatternAction =>
        val preparedMerge: MergePatternAction = prepareMergeAction(in, originalMerge, ctx, in.pipe.symbols)
        val newQuery = in.query.copy(updates = in.query.updates.replace(Unsolved(originalMerge), Unsolved(preparedMerge)))

        in.copy(query = newQuery)

      case originalForeach: ForeachAction =>
        val originalMerge: MergePatternAction = extractMergeAction(originalForeach.actions)
        val symbols: SymbolTable = collectSymbols(originalForeach, originalMerge, in)

        val preparedMerge: MergePatternAction = prepareMergeAction(in, originalMerge, ctx, symbols)
        val preparedForeach = originalForeach.copy(actions = originalForeach.actions.replace(originalMerge, preparedMerge))
        val newQuery = in.query.copy(updates = in.query.updates.replace(Unsolved(originalForeach), Unsolved(preparedForeach)))

        in.copy(query = newQuery)
    }
  }

  private def collectSymbolsFromResolvedUpdateActions(actions: Seq[UpdateAction], originalMerge: UpdateAction): Seq[(String, CypherType)] =
    actions.
    takeWhile(action => action != originalMerge). // Find all UpdateActions up to the one we are looking for
    flatMap(action => action.identifiers) // Get all symbol table changes from these actions

  private def collectSymbols(originalForeach: ForeachAction, originalMerge: MergePatternAction, in: ExecutionPlanInProgress): SymbolTable = {
    val symbolsInLoop = collectSymbolsFromResolvedUpdateActions(originalForeach.actions, originalMerge).toMap
    var symbols: SymbolTable = originalForeach.addInnerIdentifier(in.pipe.symbols)
    symbols = symbols.add(symbolsInLoop)
    symbols
  }

  private def prepareMergeAction(in: ExecutionPlanInProgress,
                         originalMerge: MergePatternAction,
                         ctx: PlanContext,
                         symbols:SymbolTable): MergePatternAction = {
    val updateActions: Seq[UpdateAction] = MergePatternBuilder.createActions(symbols, originalMerge.actions)
    val matchPipe = solveMatchQuery(in, ctx, originalMerge, symbols).pipe
    val preparedMerge = originalMerge.copy(maybeMatchPipe = Some(matchPipe), maybeUpdateActions = Some(updateActions))
    preparedMerge
  }

  private def extractMergeAction(actions: Seq[UpdateAction]) = actions.find(validAction) match {
    case Some(e: MergePatternAction) => e
    case _                           => throw new InternalException("Query plan went wrong solving MERGE inside a foreach")
  }


  private def validAction: (UpdateAction) => Boolean = {
    case m: MergePatternAction => !m.readyToExecute
    case _                     => false
  }

  private def extractFrom(updates: Seq[QueryToken[UpdateAction]], symbols: SymbolTable): UpdateAction = updates.collect {
    case Unsolved(action: MergePatternAction) if !action.readyToExecute => action
  }.headOption.getOrElse(updates.collect {
    case Unsolved(foreach: ForeachAction) if interesting(foreach, symbols) => foreach
  }.head)

  private def interesting(x: ForeachAction, symbols: SymbolTable) = x.actions.exists {
    case (x: MergePatternAction) => !x.readyToExecute && x.symbolDependenciesMet(symbols)
    case _                       => false
  }

  private def createMatchQueryFor(patterns: Seq[Pattern]): PartiallySolvedQuery = PartiallySolvedQuery.apply(
    Query.
      matches(patterns: _*).
      returns(AllIdentifiers())
  )

  private def solveMatchQuery(plan: ExecutionPlanInProgress,
                              ctx: PlanContext,
                              patternAction: MergePatternAction,
                              symbols:SymbolTable): ExecutionPlanInProgress = {
    val pipe = NullPipe(symbols, plan.pipe.executionPlanDescription)
    val matchQuery = createMatchQueryFor(patternAction.patterns)
    var planInProgress = plan.copy(query = matchQuery, pipe = pipe)

    planInProgress = matching(planInProgress, ctx)

    if (patternAction.onMatch.nonEmpty)
      planInProgress = planInProgress.copy(pipe = new ExecuteUpdateCommandsPipe(planInProgress.pipe, patternAction.onMatch))

    planInProgress
  }
}

object MergePatternBuilder {
  def createActions(in: SymbolTable, createRels: Seq[UpdateAction]): Seq[UpdateAction] = {

    var symbol = in

    createRels.flatMap {
      case rel@CreateRelationship(_, RelationshipEndpoint(Identifier(name), props, labels), _, _, _)
        if !symbol.hasIdentifierNamed(name) =>
        symbol = symbol.add(name, NodeType())
        Seq(CreateNode(name, props, labels), rel)

      case rel@CreateRelationship(_, _, RelationshipEndpoint(Identifier(name), props, labels), _, _)
        if !symbol.hasIdentifierNamed(name) =>
        symbol = symbol.add(name, NodeType())
        Seq(CreateNode(name, props, labels), rel)

      case x => Some(x)
    }
  }
}
