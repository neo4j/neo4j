/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

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

  def createResults(state: QueryState) : Iterator[CypherRow] = {
    val decoratedState = state.decorator.decorate(self.id, state)
    decoratedState.setExecutionContextFactory(executionContextFactory)
    val innerResult = internalCreateResults(decoratedState)
    state.decorator.afterCreateResults(self.id, decoratedState)
    state.decorator.decorate(self.id, innerResult, () => state.initialContext)
  }

  protected def internalCreateResults(state: QueryState): Iterator[CypherRow]

  // Used by profiling to identify where to report dbhits and rows
  def id: Id

  // TODO: Alternatively we could pass the logicalPlanId when we create contexts, and in the SlottedQueryState use the
  // SlotConfigurations map to get the slot configuration needed for the context creation,
  // but then we would get an extra map lookup at runtime every time we create a new context.
  var executionContextFactory: ExecutionContextFactory = CommunityExecutionContextFactory()
}

case class ArgumentPipe()(val id: Id = Id.INVALID_ID) extends Pipe {

  def internalCreateResults(state: QueryState) =
    Iterator(state.newExecutionContextWithInitialContext(executionContextFactory))
}

abstract class PipeWithSource(source: Pipe) extends Pipe {
  override def createResults(state: QueryState): Iterator[CypherRow] = {
    val sourceResult = source.createResults(state)

    val decoratedState = state.decorator.decorate(this.id, state)
    decoratedState.setExecutionContextFactory(executionContextFactory)
    val result = internalCreateResults(sourceResult, decoratedState)
    state.decorator.afterCreateResults(this.id, decoratedState)
    state.decorator.decorate(this.id, result, sourceResult)
  }

  protected def internalCreateResults(state: QueryState): Iterator[CypherRow] =
    throw new UnsupportedOperationException("This method should never be called on PipeWithSource")

  protected def internalCreateResults(input:Iterator[CypherRow], state: QueryState): Iterator[CypherRow]
  private[pipes] def testCreateResults(input:Iterator[CypherRow], state: QueryState): Iterator[CypherRow] =
    internalCreateResults(input, state)

  def getSource: Pipe = source
}
