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

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.Effects
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class ProduceResultsPipeTest extends CypherFunSuite {

  implicit val monitor = mock[PipeMonitor]

  test("should project needed columns") {
    val sourcePipe = mock[Pipe]
    val queryState = mock[QueryState]

    when(queryState.decorator).thenReturn(NullPipeDecorator)
    when(sourcePipe.createResults(queryState)).thenReturn(
      Iterator(
        ctx("a" -> "foo", "b" -> 10, "c" -> true, "d" -> "d"),
        ctx("a" -> "bar", "b" -> 20, "c" -> false, "d" -> "d")
      ))

    val pipe = ProduceResultsPipe(sourcePipe, Seq("a", "b", "c"))(Some(1.0))

    val result = pipe.createResults(queryState).toList

    result should equal(
      List(
        Map("a" -> "foo", "b" -> 10, "c" -> true),
        Map("a" -> "bar", "b" -> 20, "c" -> false)
      ))
  }

  test("should produce no results if child pipe produces no results") {
    val sourcePipe = mock[Pipe]
    val queryState = mock[QueryState]

    when(queryState.decorator).thenReturn(NullPipeDecorator)
    when(sourcePipe.createResults(queryState)).thenReturn(Iterator.empty)

    val pipe = ProduceResultsPipe(sourcePipe, Seq("a", "b", "c"))(Some(1.0))

    val result = pipe.createResults(queryState).toList

    result shouldBe empty
  }

  test("should have no effects because it does not touch the store") {
    val pipe = ProduceResultsPipe(mock[Pipe], Seq("a", "b", "c"))(Some(1.0))
    assert(pipe.localEffects == Effects())
  }

  private def ctx(data: (String, Any)*): ExecutionContext = {
    new ExecutionContext().newWith(Map(data: _*))
  }
}
