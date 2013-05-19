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
import org.scalatest.mock.MockitoSugar
import org.neo4j.cypher.internal.commands.expressions.Literal
import org.mockito.Mockito._
import org.neo4j.cypher.internal.ExecutionContext


class SlicePipeTest extends Assertions with MockitoSugar {
  @Test
  def should_drain_after_limit() {
    //GIVEN
    val ctx1 = ExecutionContext.from("x" -> 1)
    val ctx2 = ExecutionContext.from("x" -> 2)

    val state = QueryState.empty
    val inner = mock[Pipe]
    val sourceIter = Iterator(ctx1, ctx2)
    when(inner.createResults(state)).thenReturn(sourceIter)

    val p = new SlicePipe(source = inner, limit = Some(Literal(1)), skip = None, emptySource = true)

    //WHEN
    val result = p.createResults(state)

    //THEN
    assert(result.hasNext, "Expected non empty result")
    assert(result.next === ctx1)
    assert(result.isEmpty, "Result was not limited as expected")
    assert(sourceIter.isEmpty, "Source iterator was not emptied")
  }
}