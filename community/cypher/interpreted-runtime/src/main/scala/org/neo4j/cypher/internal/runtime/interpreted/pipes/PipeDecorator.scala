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

/*
A PipeDecorator is used to instrument calls between Pipes, and between a Pipe and the graph
 */
trait PipeDecorator {

  /**
   * Return a decorated QueryState for the given plan id.
   *
   * @param state the original query state to be decorated
   * @return the decorated query state
   */
  def decorate(planId: Id, state: QueryState): QueryState

  /**
   * This method should be called after createResults, with the decorated QueryState.
   *
   * @param state the decorated query state
   */
  def afterCreateResults(planId: Id, state: QueryState): Unit

  /**
   * Return a decorated iterator. To be used from other pipe decorators.
   *
   * @param state the decorated query state
   * @param iter  iterator to decorate
   * @return the decorated iterator
   */
  def decorate(planId: Id, state: QueryState, iter: ClosingIterator[CypherRow]): ClosingIterator[CypherRow]

  // These two are used for linenumber only

  /**
   * Called if the pipe has a source.
   *
   * @param state the decorated query state
   * @param iter  iterator to decorate
   * @return the decorated iterator
   */
  def decorate(
    planId: Id,
    state: QueryState,
    iter: ClosingIterator[CypherRow],
    sourceIter: ClosingIterator[CypherRow]
  ): ClosingIterator[CypherRow] =
    decorate(planId, state, iter)

  /**
   * @param state the decorated query state
   * @param iter  iterator to decorate
   * @return the decorated iterator
   */
  def decorate(
    planId: Id,
    state: QueryState,
    iter: ClosingIterator[CypherRow],
    previousContextSupplier: () => Option[CypherRow]
  ): ClosingIterator[CypherRow] =
    decorate(planId, state, iter)

  /**
   * Called on the root pipe. This can be useful if some decorator wants to only decorate the root pipe. It then can implement this method and ignore the others.
   */
  def decorateRoot(planId: Id, state: QueryState, iter: ClosingIterator[CypherRow]): ClosingIterator[CypherRow] = iter

  /**
   * Returns the inner decorator of this decorator. The inner decorator is used for nested expressions
   * where the `decorate` should refer to the parent pipe instead of the calling pipe.
   */
  def innerDecorator(planId: Id): PipeDecorator
}

object NullPipeDecorator extends PipeDecorator {

  override def decorate(planId: Id, state: QueryState, iter: ClosingIterator[CypherRow]): ClosingIterator[CypherRow] =
    iter

  override def decorate(planId: Id, state: QueryState): QueryState = state

  override def innerDecorator(planId: Id): PipeDecorator = NullPipeDecorator

  override def afterCreateResults(planId: Id, state: QueryState): Unit = {}
}
