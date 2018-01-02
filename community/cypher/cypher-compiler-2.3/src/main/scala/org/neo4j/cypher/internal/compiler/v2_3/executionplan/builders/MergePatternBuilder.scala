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

import org.neo4j.cypher.internal.compiler.v2_3._
import commands.{AllIdentifiers, Pattern, Query}
import commands.expressions.Identifier
import executionplan.{ExecutionPlanInProgress, Phase, PartiallySolvedQuery, PlanBuilder}
import mutation._
import org.neo4j.cypher.internal.compiler.v2_3.helpers.CollectionSupport
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import pipes._
import spi.PlanContext
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

/*
This class solves MERGE for patterns. It does this by creating an execution plan that uses normal pattern matching
to find matches for a pattern. If that step returns nothing, the missing parts of the pattern are created.

By doing it this way, we rely on already existing code to both match and create the elements.

This class prepares MergePatternAction objects to be run by creating the match pipe
*/
case class MergePatternBuilder(matching: Phase) extends PlanBuilder with CollectionSupport {
  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor): Boolean =
    apply(plan, ctx) != apply(plan, ctx)

  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor): ExecutionPlanInProgress = {
    def prepareMergeAction(symbols: SymbolTable, originalMerge: MergePatternAction): (SymbolTable, MergePatternAction) = {
      val (newSymbols,updateActions) = MergePatternBuilder.createActions(symbols, originalMerge.actions ++ originalMerge.onCreate)
      val matchPipe = solveMatchQuery(symbols, originalMerge).pipe
      val preparedMerge = originalMerge.copy(maybeMatchPipe = Some(matchPipe), maybeUpdateActions = Some(updateActions))
      (newSymbols, preparedMerge)
    }

    def solveMatchQuery(symbols: SymbolTable, patternAction: MergePatternAction)(implicit pipeMonitor: PipeMonitor): ExecutionPlanInProgress = {
      val pipe = new ArgumentPipe(symbols)() {
        override def planDescription: InternalPlanDescription = plan.pipe.planDescription
      }
      val matchQuery = createMatchQueryFor(patternAction.patterns)
      var planInProgress = plan.copy(query = matchQuery, pipe = pipe)

      planInProgress = matching(planInProgress, ctx)

      if (patternAction.onMatch.nonEmpty)
        planInProgress = planInProgress.copy(pipe = new ExecuteUpdateCommandsPipe(planInProgress.pipe, patternAction.onMatch))

      planInProgress
    }

    def unsolved(pair: (SymbolTable, UpdateAction)) = (pair._1, Unsolved(pair._2))

    def rewrite(symbols: SymbolTable, inAction: UpdateAction): (SymbolTable, UpdateAction) = inAction match {
      case mergeAction: MergePatternAction if mergeAction.maybeMatchPipe.isEmpty =>
        prepareMergeAction(symbols, mergeAction)

      case foreachAction: ForeachAction =>
        val innerSymbols = foreachAction.addInnerIdentifier(symbols)
        val (_, newActions) = foreachAction.actions.foldMap(innerSymbols)(rewrite)
        (symbols, foreachAction.copy(actions = newActions))

      case action =>
        (symbols.add(action.identifiers.toMap), action)
    }

    val (_, newUpdates) = plan.query.updates.foldMap(plan.pipe.symbols) {
      case (symbols, Unsolved(action)) if action.symbolDependenciesMet(symbols) => unsolved(rewrite(symbols, action))
      case (symbols, qt) => (symbols.add(qt.token.identifiers.toMap), qt)
    }

    plan.copy(query = plan.query.copy(updates = newUpdates))
  }

  private def createMatchQueryFor(patterns: Seq[Pattern]): PartiallySolvedQuery = PartiallySolvedQuery.apply(
    Query.
      matches(patterns: _*).
      returns(AllIdentifiers())
  )
}

object MergePatternBuilder {
  def createActions(in: SymbolTable, createRels: Seq[UpdateAction]): (SymbolTable, Seq[UpdateAction]) =
    createRels.foldLeft((in, Seq[UpdateAction]())) {
      case ((s0, acc), rel@CreateRelationship(_, lhs, rhs, _, _)) =>
        val (s1, r1) = optCreateNode(s0, lhs)
        val (s2, r2) = optCreateNode(s1, rhs)
        (s2, acc ++ r1 ++ r2 :+ rel)

      case ((symbols, acc), action) =>
        (symbols, acc :+ action)
    }

  def optCreateNode(symbols: SymbolTable, ep: RelationshipEndpoint): (SymbolTable, Option[CreateNode]) = ep match {
    case RelationshipEndpoint(Identifier(name), props, labels) if !symbols.hasIdentifierNamed(name) =>
      (symbols.add(name, CTNode), Some(CreateNode(name, props, labels)))
    case _ =>
      (symbols, None)
  }
}
