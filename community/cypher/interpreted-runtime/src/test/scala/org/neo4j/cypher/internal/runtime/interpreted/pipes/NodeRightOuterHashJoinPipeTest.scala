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
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ImplicitValueConversion.toNodeValue
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContextHelper.RichExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.TestableIterator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.ValueUtils.fromNodeEntity
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.intValue

class NodeRightOuterHashJoinPipeTest extends CypherFunSuite with NodeHashJoinPipeTestSupport {

  test("should support simple hash join over nodes") {
    // given
    val queryState = QueryStateHelper.emptyWithValueSerialization

    val right = newMockedPipe(
      row("b" -> fromNodeEntity(node1)),
      row("b" -> fromNodeEntity(node2))
    )

    val left = newMockedPipe(
      row("b" -> fromNodeEntity(node2), "a" -> intValue(2)),
      row("b" -> fromNodeEntity(node3), "a" -> intValue(3))
    )

    // when
    val result = NodeRightOuterHashJoinPipe(Set("b"), left, right, Set("a"))().createResults(queryState)

    // then
    result.map(_.toMap).toList should equal(List(
      Map("b" -> fromNodeEntity(node1), "a" -> NO_VALUE),
      Map("b" -> fromNodeEntity(node2), "a" -> intValue(2))
    ))
  }

  test("should support cached node properties") {
    // given
    val queryState = QueryStateHelper.emptyWithValueSerialization

    val bPropLeft = prop("b", "prop1")
    val left = newMockedPipe(
      rowWith("b" -> fromNodeEntity(node1)).cached(bPropLeft -> intValue(-1)),
      rowWith("b" -> fromNodeEntity(node2)).cached(bPropLeft -> intValue(-2))
    )

    val bPropRight = prop("b", "prop2")
    val right = newMockedPipe(
      rowWith("b" -> fromNodeEntity(node2)).cached(bPropRight -> intValue(12)),
      rowWith("b" -> fromNodeEntity(node3)).cached(bPropRight -> intValue(13))
    )

    // when
    val result = NodeRightOuterHashJoinPipe(Set("b"), left, right, Set.empty)().createResults(queryState).toSeq

    // then
    result.map(_.toMap) should equal(Seq(
      Map("b" -> fromNodeEntity(node2)),
      Map("b" -> fromNodeEntity(node3))
    ))
    result.map(_.getCachedProperty(bPropLeft.runtimeKey)) should be(Seq(intValue(-2), null))
    result.map(_.getCachedProperty(bPropRight.runtimeKey)) should be(Seq(intValue(12), intValue(13)))
  }

  test("should work when the inner pipe produces multiple rows with the same join key") {
    // given
    val queryState = QueryStateHelper.emptyWithValueSerialization

    val right = newMockedPipe(
      row("b" -> fromNodeEntity(node1), "a" -> intValue(10)),
      row("b" -> fromNodeEntity(node2), "a" -> intValue(20))
    )

    val left = newMockedPipe(
      row("b" -> fromNodeEntity(node2), "c" -> intValue(30)),
      row("b" -> fromNodeEntity(node2), "c" -> intValue(40))
    )

    // when
    val result = NodeRightOuterHashJoinPipe(Set("b"), left, right, Set("c"))().createResults(queryState)

    // then
    result.map(_.toMap).toList should equal(List(
      Map("a" -> intValue(10), "b" -> fromNodeEntity(node1), "c" -> NO_VALUE),
      Map("a" -> intValue(20), "b" -> fromNodeEntity(node2), "c" -> intValue(30)),
      Map("a" -> intValue(20), "b" -> fromNodeEntity(node2), "c" -> intValue(40))
    ))
  }

  test("empty rhs should give empty results and not fetch anything from the lhs") {
    // given
    val queryState = QueryStateHelper.emptyWithValueSerialization

    val right = newMockedPipe()

    val left = mock[Pipe]
    val lhsIterator = new TestableIterator(ClosingIterator(row("b" -> newMockedNode(0))))
    when(left.createResults(any())).thenReturn(lhsIterator)

    // when
    val result = NodeRightOuterHashJoinPipe(Set("b"), left, right, Set("c"))().createResults(queryState)

    // then
    result.map(_.toMap).toList shouldBe Symbol("empty")
    lhsIterator.fetched should equal(0)
  }

  test("empty lhs should give null results") {
    // given
    val queryState = QueryStateHelper.emptyWithValueSerialization

    val right = newMockedPipe(
      row("b" -> fromNodeEntity(node1), "a" -> intValue(10)),
      row("b" -> fromNodeEntity(node2), "a" -> intValue(20)),
      row("b" -> fromNodeEntity(node3), "a" -> intValue(30))
    )

    val left = newMockedPipe()

    // when
    val result = NodeRightOuterHashJoinPipe(Set("b"), left, right, Set("c"))().createResults(queryState)

    // then
    result.map(_.toMap).toList should equal(List(
      Map("a" -> intValue(10), "b" -> fromNodeEntity(node1), "c" -> NO_VALUE),
      Map("a" -> intValue(20), "b" -> fromNodeEntity(node2), "c" -> NO_VALUE),
      Map("a" -> intValue(30), "b" -> fromNodeEntity(node3), "c" -> NO_VALUE)
    ))
  }

  test("rhs with null in the join key should not match anything on lhs") {
    // given
    val queryState = QueryStateHelper.emptyWithValueSerialization

    val right = newMockedPipe(
      row("b" -> fromNodeEntity(node1), "a" -> intValue(10)),
      row("b" -> NO_VALUE, "a" -> intValue(20)),
      row("b" -> fromNodeEntity(node3), "a" -> intValue(30))
    )

    val left = newMockedPipe(
      row("b" -> fromNodeEntity(node1), "c" -> intValue(10)),
      row("b" -> fromNodeEntity(node2), "c" -> intValue(20)),
      row("b" -> fromNodeEntity(node3), "c" -> intValue(30))
    )

    // when
    val result = NodeRightOuterHashJoinPipe(Set("b"), left, right, Set("c"))().createResults(queryState)

    // then
    result.map(_.toMap).toList should equal(List(
      Map("a" -> intValue(10), "b" -> fromNodeEntity(node1), "c" -> intValue(10)),
      Map("a" -> intValue(20), "b" -> NO_VALUE, "c" -> NO_VALUE),
      Map("a" -> intValue(30), "b" -> fromNodeEntity(node3), "c" -> intValue(30))
    ))
  }

  test("lhs with null in the join key should not match anything") {
    // given
    val queryState = QueryStateHelper.emptyWithValueSerialization

    val right = newMockedPipe(
      row("b" -> fromNodeEntity(node1), "a" -> intValue(10)),
      row("b" -> fromNodeEntity(node2), "a" -> intValue(20)),
      row("b" -> fromNodeEntity(node3), "a" -> intValue(30))
    )

    val left = newMockedPipe(
      row("b" -> NO_VALUE, "c" -> intValue(10)),
      row("b" -> fromNodeEntity(node2), "c" -> intValue(20)),
      row("b" -> fromNodeEntity(node3), "c" -> intValue(30))
    )

    // when
    val result = NodeRightOuterHashJoinPipe(Set("b"), left, right, Set("c"))().createResults(queryState)

    // then
    result.map(_.toMap).toList should equal(List(
      Map("a" -> intValue(10), "b" -> fromNodeEntity(node1), "c" -> NO_VALUE),
      Map("a" -> intValue(20), "b" -> fromNodeEntity(node2), "c" -> intValue(20)),
      Map("a" -> intValue(30), "b" -> fromNodeEntity(node3), "c" -> intValue(30))
    ))
  }

  test("null in both sides should still not match anything") {
    // given
    val queryState = QueryStateHelper.emptyWithValueSerialization

    val right = newMockedPipe(
      row("b" -> NO_VALUE, "a" -> intValue(20))
    )

    val left = newMockedPipe(
      row("b" -> NO_VALUE, "c" -> intValue(20))
    )

    // when
    val result = NodeRightOuterHashJoinPipe(Set("b"), left, right, Set("c"))().createResults(queryState)

    // then
    result.map(_.toMap).toList should equal(List(
      Map("a" -> intValue(20), "b" -> NO_VALUE, "c" -> NO_VALUE)
    ))
  }

  test("should support joining on two different variables") {
    // given
    val queryState = QueryStateHelper.emptyWithValueSerialization

    val right = newMockedPipe(
      row("a" -> fromNodeEntity(node1), "b" -> fromNodeEntity(node2), "c" -> intValue(1)),
      row("a" -> fromNodeEntity(node1), "b" -> fromNodeEntity(node3), "c" -> intValue(2)),
      row("a" -> fromNodeEntity(node1), "b" -> fromNodeEntity(node3), "c" -> intValue(3)),
      row("a" -> fromNodeEntity(node2), "b" -> fromNodeEntity(node3), "c" -> intValue(4)),
      row("a" -> fromNodeEntity(node1), "b" -> NO_VALUE, "c" -> intValue(5))
    )

    val left = newMockedPipe(
      row("a" -> fromNodeEntity(node1), "b" -> fromNodeEntity(node2), "d" -> intValue(1)),
      row("a" -> fromNodeEntity(node1), "b" -> fromNodeEntity(node3), "d" -> intValue(2)),
      row("a" -> fromNodeEntity(node3), "b" -> fromNodeEntity(node3), "d" -> intValue(3)),
      row("a" -> NO_VALUE, "b" -> fromNodeEntity(node3), "d" -> intValue(4))
    )

    // when
    val result = NodeRightOuterHashJoinPipe(Set("a", "b"), left, right, Set("d"))().createResults(queryState)

    // then
    result.map(_.toMap).toList should equal(List(
      Map("a" -> fromNodeEntity(node1), "b" -> fromNodeEntity(node2), "c" -> intValue(1), "d" -> intValue(1)),
      Map("a" -> fromNodeEntity(node1), "b" -> fromNodeEntity(node3), "c" -> intValue(2), "d" -> intValue(2)),
      Map("a" -> fromNodeEntity(node1), "b" -> fromNodeEntity(node3), "c" -> intValue(3), "d" -> intValue(2)),
      Map("a" -> fromNodeEntity(node2), "b" -> fromNodeEntity(node3), "c" -> intValue(4), "d" -> NO_VALUE),
      Map("a" -> fromNodeEntity(node1), "b" -> NO_VALUE, "c" -> intValue(5), "d" -> NO_VALUE)
    ))
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
    NodeRightOuterHashJoinPipe(Set("n"), left, right, Set())().createResults(queryState).toList

    // then
    monitor.closedResources.collect { case t: ProbeTable => t } should have size (1)
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
    val result = NodeRightOuterHashJoinPipe(Set("n"), left, right, Set())().createResults(queryState)
    result.close()

    // then
    monitor.closedResources.collect { case t: ProbeTable => t } should have size (1)
  }
}
