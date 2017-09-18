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

import org.mockito.Mockito._
import org.mockito.internal.stubbing.defaultanswers.ReturnsMocks
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Literal
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlanId

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

  override def id: LogicalPlanId = ???

  override def createResults(state: QueryState): Iterator[ExecutionContext] = inputIterator
}
