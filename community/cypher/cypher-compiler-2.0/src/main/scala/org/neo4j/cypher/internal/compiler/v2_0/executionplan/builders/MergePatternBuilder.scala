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
import org.neo4j.cypher.internal.compiler.v2_0.symbols.NodeType
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.ExecutionPlanInProgress
import org.neo4j.cypher.internal.compiler.v2_0.commands.AllIdentifiers
import org.neo4j.cypher.internal.compiler.v2_0.symbols.SymbolTable

/*
This class solves MERGE for patterns. It does this by creating an execution plan that uses normal pattern matching
to find matches for a pattern. If that step returns nothing, the missing parts of the pattern are created.

By doing it this way, we rely on already existing code to both match and create the elements.

This class prepares MergePatternAction objects to be run by creating the match pipe
*/
case class MergePatternBuilder(matching: Phase) extends PlanBuilder {
  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext): Boolean =
    plan.query.updates.exists {
      case Unsolved(x: MergePatternAction) => x.updateActions.isEmpty && x.symbolDependenciesMet(plan.pipe.symbols)
      case _                               => false
    }

  def apply(in: ExecutionPlanInProgress, ctx: PlanContext): ExecutionPlanInProgress = {
    val originalMerge: MergePatternAction = extractFrom(in.query.updates)

    val updateActions: Seq[UpdateAction] = MergePatternBuilder.createActions(in.pipe.symbols, originalMerge.actions)
    val matchPipe = solveMatchQuery(in, ctx, originalMerge).pipe
    val preparedMerge = originalMerge.copy(matchPipe = Some(matchPipe), updateActions = Some(updateActions))
    val newQuery = in.query.copy(updates = in.query.updates.replace(Unsolved(originalMerge), Unsolved(preparedMerge)))

    in.copy(query = newQuery)
  }

  private def extractFrom(updates: Seq[QueryToken[UpdateAction]]): MergePatternAction = updates.collect {
    case Unsolved(action: MergePatternAction) => action
  }.head

  private def createMatchQueryFor(patterns: Seq[Pattern]): PartiallySolvedQuery = PartiallySolvedQuery.apply(
    Query.
      matches(patterns: _*).
      returns(AllIdentifiers())
  )

  private def solveMatchQuery(plan: ExecutionPlanInProgress, ctx: PlanContext, patternAction: MergePatternAction): ExecutionPlanInProgress = {
    val pipe = NullPipe(plan.pipe.symbols, plan.pipe.executionPlanDescription)
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
      case rel@CreateRelationship(_, RelationshipEndpoint(Identifier(name), props, labels, bare), _, _, _)
        if !symbol.hasIdentifierNamed(name) =>
        symbol = symbol.add(name, NodeType())
        Seq(CreateNode(name, props, labels, bare), rel)

      case rel@CreateRelationship(_, _, RelationshipEndpoint(Identifier(name), props, labels, bare), _, _)
        if !symbol.hasIdentifierNamed(name) =>
        symbol = symbol.add(name, NodeType())
        Seq(CreateNode(name, props, labels, bare), rel)

      case x => Some(x)
    }
  }
}
