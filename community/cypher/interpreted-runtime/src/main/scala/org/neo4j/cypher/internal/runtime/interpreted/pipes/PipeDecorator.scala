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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription

/*
A PipeDecorator is used to instrument calls between Pipes, and between a Pipe and the graph
 */
trait PipeDecorator {
  def decorate(pipe: Pipe, state: QueryState): QueryState

  def decorate(pipe: Pipe, iter: Iterator[ExecutionContext]): Iterator[ExecutionContext]

  def decorate(plan: () => InternalPlanDescription, verifyProfileReady: () => Unit): () => InternalPlanDescription

  /*
   * Returns the inner decorator of this decorator. The inner decorator is used for nested expressions
   * where the `decorate` should refer to the parent pipe instead of the calling pipe.
   */
  def innerDecorator(pipe: Pipe): PipeDecorator
}

object NullPipeDecorator extends PipeDecorator {
  def decorate(pipe: Pipe, iter: Iterator[ExecutionContext]): Iterator[ExecutionContext] = iter

  def decorate(plan: () => InternalPlanDescription, verifyProfileReady: () => Unit): () => InternalPlanDescription = plan

  def decorate(pipe: Pipe, state: QueryState): QueryState = state

  def innerDecorator(pipe: Pipe): PipeDecorator = NullPipeDecorator
}
