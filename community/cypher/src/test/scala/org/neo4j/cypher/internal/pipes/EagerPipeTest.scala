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
package org.neo4j.cypher.internal.pipes

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.cypher.PlanDescription
import org.neo4j.cypher.internal.symbols.SymbolTable
import org.neo4j.cypher.internal.ExecutionContext

class EagerPipeTest extends Assertions {
  @Test
  def shouldMakeLazyEager() {
    // Given a lazy iterator that is not empty
    val lazyIterator = new LazyIterator[ExecutionContext](10, ExecutionContext.empty)
    val src = new FakePipe(lazyIterator)
    val eager = new EagerPipe(src)
    assert(lazyIterator.nonEmpty, "Should not be empty")

    // When
    val resultIterator = eager.createResults(QueryState.empty)

    // Then the lazy iterator is emptied, and the returned iterator is not
    assert(lazyIterator.isEmpty, "Should be empty")
    assert(resultIterator.nonEmpty, "Should not be empty")
  }

  case class FakePipe(data: Iterator[ExecutionContext]) extends Pipe {
    protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = data

    def symbols: SymbolTable = new SymbolTable()

    def executionPlanDescription: PlanDescription = ???
  }

  class LazyIterator[T](count: Int, f: => T) extends Iterator[T]() {

    var counter = 0

    def hasNext: Boolean = counter < count

    def next(): T = {
      counter += 1
      f
    }

    override def toString(): String = counter.toString
  }

}