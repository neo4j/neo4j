/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.util.attribution.Id

/**
 * Pipe is a central part of Cypher. Most pipes are decorators - they
 * wrap another pipe. ParamPipe and NullPipe the only exception to this.
 * Pipes are combined to form an execution plan, and when iterated over,
 * the execute the query.
 *
 * ** WARNING **
 * Pipes are re-used between query executions, and must not hold state in instance fields.
 * Not heeding this warning will lead to bugs that do not manifest except for under concurrent use.
 * If you need to keep state per-query, have a look at QueryState instead.
 */
trait Pipe {
  self: Pipe =>

  def createResults(state: QueryState): ClosingIterator[CypherRow] = {
    val decoratedState = state.decorator.decorate(self.id, state)
    decoratedState.setExecutionContextFactory(rowFactory)
    val innerResult = internalCreateResults(decoratedState)
    state.decorator.afterCreateResults(self.id, decoratedState)
    val decoratedResult = state.decorator.decorate(self.id, decoratedState, innerResult, () => state.initialContext)

    if (isRootPipe) {
      state.decorator.decorateRoot(this.id, decoratedState, decoratedResult)
    } else {
      decoratedResult
    }
  }

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow]

  // Used by profiling to identify where to report dbhits and rows
  def id: Id

  // TODO: Alternatively we could pass the logicalPlanId when we create contexts, and in the SlottedQueryState use the
  // SlotConfigurations map to get the slot configuration needed for the context creation,
  // but then we would get an extra map lookup at runtime every time we create a new context.
  var rowFactory: CypherRowFactory = CommunityCypherRowFactory()

  /**
   * True iff this pipe does not have a parent pipe but is the root of the pipe tree.
   */
  def isRootPipe: Boolean = false
}

case class ArgumentPipe()(val id: Id = Id.INVALID_ID) extends Pipe {

  def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] =
    ClosingIterator.single(state.newRowWithArgument(rowFactory))
}

abstract class PipeWithSource(source: Pipe) extends Pipe {

  final override def createResults(state: QueryState): ClosingIterator[CypherRow] = {
    val decoratedState = decorateState(state)

    val decoratedResult = computeDecoratedResult(state, decoratedState)

    if (isRootPipe) {
      state.decorator.decorateRoot(this.id, decoratedState, decoratedResult)
    } else {
      decoratedResult
    }
  }

  protected def computeDecoratedResult(state: QueryState, decoratedState: QueryState): ClosingIterator[CypherRow] = {
    val sourceResult = source.createResults(state)
    decorateResult(sourceResult, decoratedState, internalCreateResults(sourceResult, decoratedState))
  }

  final def decorateResult(
    sourceResult: ClosingIterator[CypherRow],
    decoratedState: QueryState,
    result: ClosingIterator[CypherRow]
  ): ClosingIterator[CypherRow] = {
    decoratedState.decorator.afterCreateResults(this.id, decoratedState)
    val decoratedResult =
      decoratedState.decorator.decorate(this.id, decoratedState, result, sourceResult).closing(sourceResult)
    decoratedResult
  }

  final def decorateState(state: QueryState): QueryState = {
    val decoratedState = state.decorator.decorate(this.id, state)
    decoratedState.setExecutionContextFactory(rowFactory)
    decoratedState
  }

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] =
    throw new UnsupportedOperationException("This method should never be called on PipeWithSource")

  protected def internalCreateResults(input: ClosingIterator[CypherRow], state: QueryState): ClosingIterator[CypherRow]

  private[pipes] def testCreateResults(input: ClosingIterator[CypherRow], state: QueryState) =
    internalCreateResults(input, state)

  def getSource: Pipe = source
}
