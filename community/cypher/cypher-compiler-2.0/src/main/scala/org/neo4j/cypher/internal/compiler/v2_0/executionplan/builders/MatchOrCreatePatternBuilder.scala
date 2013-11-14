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
import org.neo4j.cypher.internal.compiler.v2_0.pipes.{ExecuteUpdateCommandsPipe, Pipe}
import org.neo4j.cypher.internal.compiler.v2_0.commands.{Pattern, Query, AllIdentifiers}
import org.neo4j.cypher.internal.compiler.v2_0.ExecutionContext
import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Identifier
import org.neo4j.cypher.internal.compiler.v2_0.symbols.{NodeType, SymbolTable}
import org.neo4j.cypher.internal.compiler.v2_0.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_0.mutation.CreateRelationship
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.ExecutionPlanInProgress
import org.neo4j.cypher.internal.compiler.v2_0.mutation.MergePatternAction
import org.neo4j.cypher.internal.compiler.v2_0.pipes.optional.InsertingPipe

/*
This class solves MERGE for patterns. It does this by creating an execution plan that uses normal pattern matching
to find matches for a pattern. If that step returns nothing, the missing parts of the pattern are created.

By doing it this way, we rely on already existing code to both match and create the elements.
 */
case class MatchOrCreatePatternBuilder(matching: Phase) extends PlanBuilder {
  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext): Boolean =
    plan.query.updates.exists {
      case Unsolved(_: MergePatternAction) => true
      case _ => false
    }


  def apply(in: ExecutionPlanInProgress, ctx: PlanContext): ExecutionPlanInProgress = {
    val patternAction: MergePatternAction = in.query.updates.collect {
      case Unsolved(action: MergePatternAction) => action
    }.head

    val matchQuery = createMatchQueryFor(patternAction.patterns)

    val updateActions: Seq[UpdateAction] = MatchOrCreatePatternBuilder.createActions(in.pipe.symbols, patternAction.actions)
    val noMatchFunction = runUpdateCommands(_: ExecutionContext, _: Seq[String], _: QueryState, updateActions)

    val matchOrCreatePipe = new InsertingPipe(in = in.pipe,
      builder = builder(_: Pipe, in.copy(query = matchQuery), ctx, patternAction),
      noMatch = noMatchFunction)

    val newQuery = in.query.copy(updates = in.query.updates.replace(Unsolved(patternAction), Solved(patternAction)))

    in.copy(pipe = matchOrCreatePipe, query = newQuery, isUpdating = true)
  }

  private def createMatchQueryFor(patterns: Seq[Pattern]): PartiallySolvedQuery = PartiallySolvedQuery.apply(
    Query.
      matches(patterns: _*).
      returns(AllIdentifiers())
  )

  private def builder(in: Pipe, plan: ExecutionPlanInProgress, ctx: PlanContext, patternAction: MergePatternAction): Pipe = {
    var planInProgress = plan.copy(pipe = in)
    planInProgress = matching(planInProgress, ctx)
    if (patternAction.onMatch.isEmpty)
      planInProgress.pipe
    else
      new ExecuteUpdateCommandsPipe(planInProgress.pipe, patternAction.onMatch)
  }

  private def runUpdateCommands(in: ExecutionContext, introducedIdentifiers: Seq[String], queryState: QueryState, updateActions: Seq[UpdateAction]): ExecutionContext =
  // Runs all commands, from left to right, updating the execution context as it passes through
    updateActions.foldLeft(in) {
      case (ctx, action) => action.exec(ctx, queryState).single // The single on the end is because an UpdateAction returns an Iterator, and we make sure that it's only created a single element
    }

  implicit class SingleIterator[T](inner: Iterator[T]) {
    def single: T = {
      val temp = inner.next()

      if (inner.hasNext)
        throw new InternalException("Creating a missing element in MERGE resulted in multiple elements")

      temp
    }
  }

}

object MatchOrCreatePatternBuilder {
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
