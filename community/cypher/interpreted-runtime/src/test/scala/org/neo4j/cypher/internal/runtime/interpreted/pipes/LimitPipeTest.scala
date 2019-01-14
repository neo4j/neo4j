/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.mockito.Mockito._
import org.mockito.internal.stubbing.defaultanswers.ReturnsMocks
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, QueryStateHelper}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

class LimitPipeTest extends CypherFunSuite {
  test("limit 0 should not pull anything from the incoming iterator") {
    // Given
    val inputIterator = mock[Iterator[ExecutionContext]](new ReturnsMocks)

    when(inputIterator.isEmpty).thenReturn(false)

    val src: Pipe = new DummyPipe(inputIterator)
    val limitPipe = LimitPipe(src, Literal(0))()

    // When
    limitPipe.createResults(QueryStateHelper.empty)

    // Then
    verify(inputIterator, never()).next()
  }
}

class DummyPipe(inputIterator: Iterator[ExecutionContext]) extends Pipe {
  override protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = ???

  override def id(): Id = ???

  override def createResults(state: QueryState): Iterator[ExecutionContext] = inputIterator
}
