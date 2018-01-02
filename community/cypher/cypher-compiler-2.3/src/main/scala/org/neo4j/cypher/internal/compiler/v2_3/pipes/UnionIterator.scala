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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3._

class UnionIterator(in: Seq[Pipe], state: QueryState) extends Iterator[ExecutionContext] {

  /*
  This field can have one of three states:
    null    -> the next value has not yet been fetched from the underlying Pipes
    None    -> this iterator has been emptied
    Some(x) -> the next value has been fetched, but not yet seen by next()
  */
  var currentValue: Option[ExecutionContext] = null

  /*
  Before the first pipe has been applied, currentIterator will have null in it. After that it will have an
  iterator in it always.
   */
  var currentIterator: Iterator[ExecutionContext] = null
  var pipesLeft: List[Pipe] = in.toList

  def hasNext: Boolean = {
    stepIfNeccessary()
    currentValue.nonEmpty
  }

  def next(): ExecutionContext = {
    stepIfNeccessary()

    val result = currentValue.getOrElse(Iterator.empty.next())
    currentValue = null
    result
  }

  private def stepIfNeccessary() {
    def loadNextIterator() {
      val p = pipesLeft.head
      pipesLeft = pipesLeft.tail

      currentIterator = p.createResults(state)
    }

    def step() {
      if (currentIterator == null)
        loadNextIterator()

      while (currentIterator.isEmpty && pipesLeft.nonEmpty) {
        loadNextIterator()
      }

      if (currentIterator.hasNext) {
        currentValue = Some(currentIterator.next())
      } else {
        currentValue = None
      }
    }

    if (currentValue == null) {
      step()
    }
  }
}
