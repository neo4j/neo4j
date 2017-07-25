/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id

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

  def createResults(state: QueryState) : Iterator[ExecutionContext] = {
    val decoratedState = state.decorator.decorate(self, state)
    val innerResult = internalCreateResults(decoratedState)
    state.decorator.decorate(self, innerResult)
  }

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext]

  // Used by profiling to identify where to report dbhits and rows
  def id: Id
}

case class SingleRowPipe()(val id: Id = new Id) extends Pipe {

  def internalCreateResults(state: QueryState) =
    Iterator(state.createOrGetInitialContext())
}

abstract class PipeWithSource(source: Pipe) extends Pipe {
  override def createResults(state: QueryState): Iterator[ExecutionContext] = {
    val sourceResult = source.createResults(state)

    val decoratedState = state.decorator.decorate(this, state)
    val result = internalCreateResults(sourceResult, decoratedState)
    state.decorator.decorate(this, result)
  }

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] =
    throw new UnsupportedOperationException("This method should never be called on PipeWithSource")

  protected def internalCreateResults(input:Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext]
}
