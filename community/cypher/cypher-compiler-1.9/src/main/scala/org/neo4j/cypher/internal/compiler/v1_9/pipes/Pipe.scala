/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v1_9.pipes

import org.neo4j.cypher.internal.compiler.v1_9.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.compiler.v1_9.executionplan.PlanDescription

/**
 * Pipe is a central part of Cypher. Most pipes are decorators - they
 * wrap another pipe. ParamPipe and NullPipe the only exception to this.
 * Pipes are combined to form an execution plan, and when iterated over,
 * the execute the query.
 */
trait Pipe {
  def createResults(state: QueryState) : Iterator[ExecutionContext] = {
    val decoratedState = state.decorator.decorate(this, state)
    val result = internalCreateResults(decoratedState)
    state.decorator.decorate(this, result)
  }

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext]

  def symbols: SymbolTable

  def executionPlanDescription: PlanDescription

  /**
   * Please make sure to add a test for this implementation @ PipeLazynessTest
   */
  def isLazy:Boolean = true
}

object NullPipe extends Pipe {
  def internalCreateResults(state: QueryState) = Iterator.single(ExecutionContext.empty)

  val symbols: SymbolTable = SymbolTable()

  val executionPlanDescription = PlanDescription(this, "Null")
}

abstract class PipeWithSource(val source: Pipe) extends Pipe {
  def throwIfSymbolsMissing(symbols: SymbolTable)

  throwIfSymbolsMissing(source.symbols)

  override def createResults(state: QueryState): Iterator[ExecutionContext] = {
    val sourceResult = source.createResults(state)

    val decoratedState = state.decorator.decorate(this, state)
    val result = internalCreateResults(sourceResult, decoratedState)
    state.decorator.decorate(this, result)
  }

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] =
    throw new ThisShouldNotHappenError("Andres", "This method should never be called on PipeWithSource")

  protected def internalCreateResults(input:Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext]
}
