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
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ImplicitValueConversion._
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.{FALSE, TRUE, intValue, stringValue}

class ProduceResultsPipeTest extends CypherFunSuite {

  test("should project needed columns") {
    val sourcePipe = mock[Pipe]
    val queryState = mock[QueryState]

    when(queryState.decorator).thenReturn(NullPipeDecorator)
    when(sourcePipe.createResults(queryState)).thenReturn(
      Iterator(
        ExecutionContext.from("a" -> "foo", "b" -> 10, "c" -> true, "d" -> "d"),
        ExecutionContext.from("a" -> "bar", "b" -> 20, "c" -> false, "d" -> "d")
      ))

    val pipe = ProduceResultsPipe(sourcePipe, Seq("a", "b", "c"))()

    val result = pipe.createResults(queryState).toList

    result should equal(
      List(
        Map("a" -> stringValue("foo"), "b" -> intValue(10), "c" -> TRUE),
        Map("a" -> stringValue("bar"), "b" -> intValue(20), "c" -> FALSE)
      ))
  }

  test("should produce no results if child pipe produces no results") {
    val sourcePipe = mock[Pipe]
    val queryState = mock[QueryState]

    when(queryState.decorator).thenReturn(NullPipeDecorator)
    when(sourcePipe.createResults(queryState)).thenReturn(Iterator.empty)

    val pipe = ProduceResultsPipe(sourcePipe, Seq("a", "b", "c"))()

    val result = pipe.createResults(queryState).toList

    result shouldBe empty
  }
}
