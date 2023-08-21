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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ImplicitValueConversion.toNodeValue
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.TestableIterator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.kernel.impl.util.collection
import org.neo4j.values.AnyValue

class NodeHashJoinPipeTest extends CypherFunSuite {

  test("should not fetch results from RHS if LHS is empty") {
    // given
    val queryState = QueryStateHelper.empty

    val left = mock[Pipe]
    when(left.createResults(queryState)).thenReturn(ClosingIterator.empty)

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
    when(left.createResults(queryState)).thenReturn(ClosingIterator(Iterator(row("b" -> null), row("b" -> null))))

    val right = mock[Pipe]
    val rhsIterator = new TestableIterator(ClosingIterator(row("b" -> newMockedNode(0))))
    when(left.createResults(queryState)).thenReturn(ClosingIterator.empty)

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

    val lhsIterator = new TestableIterator(ClosingIterator(row("b" -> node1), row("b" -> node2)))
    when(left.createResults(queryState)).thenReturn(lhsIterator)

    val right = mock[Pipe]
    when(right.createResults(queryState)).thenReturn(ClosingIterator.empty)

    // when
    val result = NodeHashJoinPipe(Set("b"), left, right)().createResults(queryState)

    // then
    result shouldBe empty
    lhsIterator.fetched should equal(0)
  }

  test("exhaust should close table") {
    // given
    val monitor = QueryStateHelper.trackClosedMonitor
    val queryState = QueryStateHelper.emptyWithResourceManager(new ResourceManager(monitor))

    val node1 = newMockedNode(1)
    val node2 = newMockedNode(2)

    val left = new FakePipe(Seq(Map("n" -> node1), Map("n" -> node2)))
    val right = new FakePipe(Seq(Map("n" -> node1), Map("n" -> node2)))

    // when
    NodeHashJoinPipe(Set("n"), left, right)().createResults(queryState).toList

    // then
    monitor.closedResources.collect { case t: collection.ProbeTable[_, _] => t } should have size (1)
  }

  test("close should close table") {
    // given
    val monitor = QueryStateHelper.trackClosedMonitor
    val queryState = QueryStateHelper.emptyWithResourceManager(new ResourceManager(monitor))

    val node1 = newMockedNode(1)
    val node2 = newMockedNode(2)

    val left = new FakePipe(Seq(Map("n" -> node1), Map("n" -> node2)))
    val right = new FakePipe(Seq(Map("n" -> node1), Map("n" -> node2)))

    // when
    val result = NodeHashJoinPipe(Set("n"), left, right)().createResults(queryState)
    result.close()

    // then
    monitor.closedResources.collect { case t: collection.ProbeTable[_, _] => t } should have size (1)
  }

  private def row(values: (String, AnyValue)*) = CypherRow.from(values: _*)

  private def newMockedNode(id: Int) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    when(node.getElementId).thenReturn(id.toString)
    when(node.toString).thenReturn("node - " + id.toString)
    node
  }

}
