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
package org.neo4j.cypher.internal.compiler.v2_0.pipes

import org.neo4j.cypher.internal.compiler.v2_0.mutation.UpdateAction
import org.neo4j.cypher.internal.compiler.v2_0.{ExecutionContext, PlanDescription}
import org.neo4j.cypher.internal.compiler.v2_0.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders.{IfElseIterator, QueryStateSettingIterator}
import org.neo4j.graphdb.Node
import org.neo4j.cypher.InternalException

case class MatchOrCreatePipe(source: Pipe, matchPipe: Pipe, updateActions: Seq[UpdateAction])
  extends PipeWithSource(source) {
  def executionPlanDescription: PlanDescription =
    source.executionPlanDescription.andThenWrap(this, "MatchOrCreate", matchPipe.executionPlanDescription)

  def throwIfSymbolsMissing(symbols: SymbolTable): Unit = {}

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    val listeningIterator = new QueryStateSettingIterator(input, state)

    val elseClause: (ExecutionContext) => Iterator[ExecutionContext] =
      lockAndThenMatch(state) _ orElse
        createTheElements(state)

    new IfElseIterator(input = listeningIterator, ifClause = doMatch(state), elseClause, () => state.initialContext = None)
  }

  private def doMatch(state: QueryState)(ctx: ExecutionContext) = matchPipe.createResults(state)

  private def lockAndThenMatch(state: QueryState)(ctx: ExecutionContext): Iterator[ExecutionContext] = {
    val lockingQueryContext = state.query.upgradeToLockingQueryContext
    ctx.collect { case (_, node: Node) => node.getId }.toSeq.sorted.
      foreach( id => lockingQueryContext.getLabelsForNode(id) ) // TODO: This locks the nodes. Hack!
    matchPipe.createResults(state)
  }

  private def createTheElements(state: QueryState)(ctx: ExecutionContext) = {
    // Runs all commands, from left to right, updating the execution context as it passes through
    val resultingContext = updateActions.foldLeft(ctx) {
      case (ctx, action) => singleElementOrFail(action.exec(ctx, state))
    }
    Iterator(resultingContext)
  }

  private def singleElementOrFail(inner: Iterator[ExecutionContext]): ExecutionContext = {
    val temp = inner.next()

    if (inner.hasNext)
      throw new InternalException("Creating a missing element in MERGE resulted in multiple elements")

    temp
  }

  implicit class OrElse[T](inner: T => Iterator[T]) {
    def orElse(other: T => Iterator[T]): T => Iterator[T] =
      new Function[T, Iterator[T]] {
        def apply(value: T): Iterator[T] = {
          val result = inner(value)
          if (result.nonEmpty)
            return result

          other(value)
        }
      }
  }

  def symbols: SymbolTable = matchPipe.symbols
}
