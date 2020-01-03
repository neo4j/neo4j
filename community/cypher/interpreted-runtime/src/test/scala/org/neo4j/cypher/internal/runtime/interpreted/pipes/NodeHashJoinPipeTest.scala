/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.ImplicitValueConversion._
import org.neo4j.cypher.internal.runtime.interpreted.{QueryStateHelper, TestableIterator}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.values.AnyValue

class NodeHashJoinPipeTest extends CypherFunSuite {

  test("should not fetch results from RHS if LHS is empty") {
    // given
    val queryState = QueryStateHelper.empty

    val left = mock[Pipe]
    when(left.createResults(queryState)).thenReturn(Iterator.empty)

    val right = mock[Pipe]

    // when
    val result = NodeHashJoinPipe(Set("b"), left, right)().createResults(queryState)

    // then
    result shouldBe empty
    verify(right, never()).createResults(any())
  }

  test("should not fetch results from RHS if no probe table was built") {
    // given
    val queryState = QueryStateHelper.empty

    val left = mock[Pipe]
    when(left.createResults(queryState)).thenReturn(Iterator(row("b" -> null), row("b" -> null)))

    val right = mock[Pipe]
    val rhsIterator = new TestableIterator(Iterator(row("b" -> newMockedNode(0))))
    when(left.createResults(queryState)).thenReturn(Iterator.empty)

    // when
    val result = NodeHashJoinPipe(Set("b"), left, right)().createResults(queryState)

    // then
    result shouldBe empty
    rhsIterator.fetched should equal(0)
  }

  test("if RHS is empty, terminate building of the probe map early") {
    // given
    val queryState = QueryStateHelper.empty

    val left = mock[Pipe]
    val node1 = newMockedNode(1)
    val node2 = newMockedNode(2)

    val lhsIterator = new TestableIterator(Iterator(row("b" -> node1), row("b" -> node2)))
    when(left.createResults(queryState)).thenReturn(lhsIterator)

    val right = mock[Pipe]
    when(right.createResults(queryState)).thenReturn(Iterator.empty)

    // when
    val result = NodeHashJoinPipe(Set("b"), left, right)().createResults(queryState)

    // then
    result shouldBe empty
    lhsIterator.fetched should equal(0)
  }

  private def row(values: (String, AnyValue)*) = ExecutionContext.from(values: _*)

  private def newMockedNode(id: Int) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    when(node.toString).thenReturn("node - " + id.toString)
    node
  }

}

