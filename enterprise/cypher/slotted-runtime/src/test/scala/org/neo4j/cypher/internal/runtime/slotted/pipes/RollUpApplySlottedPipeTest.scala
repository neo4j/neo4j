/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.FakeEntityTestSupport
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeTestSupport, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, QueryStateHelper}
import org.neo4j.cypher.internal.runtime.slotted.expressions.{NodeFromSlot, ReferenceFromSlot}
import org.neo4j.cypher.internal.runtime.{Operations, QueryContext}
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.ValueUtils.fromNodeProxy
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.{NodeValue, VirtualValues}

class RollUpApplySlottedPipeTest extends CypherFunSuite with PipeTestSupport with FakeEntityTestSupport {
  private val slots = SlotConfiguration
    .empty
    .newReference("a", nullable = true, CTNumber)
    .newReference("x", nullable = false, CTList(CTNumber)) // NOTE: This has to be last since that is the order in which the slots are assumed to be allocated

  private val collectionRefSlotOffset = slots.getReferenceOffsetFor("x")

  test("when rhs returns nothing, an empty collection should be produced") {
    // given
    val lhs = createLhs(1)
    val rhs = pipeWithResults { (state) => Iterator() }
    val pipe = RollUpApplySlottedPipe(lhs, rhs, collectionRefSlotOffset,
      identifierToCollect = ("y" -> ReferenceFromSlot(0)),
      nullableIdentifiers = Set("a"), slots)()

    // when
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList

    // then
    val a_offset = slots.get("a").get.offset
    val x_offset = slots.get("x").get.offset
    result.head.getRefAt(a_offset) should equal(Values.longValue(1))
    result.head.getRefAt(x_offset) should equal(VirtualValues.EMPTY_LIST)
  }

  test("when rhs has null values on nullableIdentifiers, a null value should be produced") {
    // given
    val lhs = createLhs(null, 1)
    val rhs = pipeWithResults { (state) => Iterator() }
    val pipe = RollUpApplySlottedPipe(lhs, rhs, collectionRefSlotOffset,
      identifierToCollect = ("y" -> ReferenceFromSlot(0)),
      nullableIdentifiers = Set("a"), slots)()

    // when
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList

    // then
    val a_offset = slots.get("a").get.offset
    val x_offset = slots.get("x").get.offset
    result(0).getRefAt(a_offset) should equal(NO_VALUE)
    result(0).getRefAt(x_offset) should equal(NO_VALUE)
    result(1).getRefAt(a_offset) should equal(Values.intValue(1))
    result(1).getRefAt(x_offset) should equal(VirtualValues.EMPTY_LIST)
  }

  test("when rhs produces multiple rows with values, they are turned into a collection") {
    // given
    val lhs = createLhs(1)
    val rhs = createRhs(1, 2, 3, 4)
    val yOffset = rhs.slots.getReferenceOffsetFor("y")
    val pipe = RollUpApplySlottedPipe(lhs, rhs, collectionRefSlotOffset,
      identifierToCollect = ("y" -> ReferenceFromSlot(yOffset)),
      nullableIdentifiers = Set("a"), slots)()

    // when
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList

    // then
    val a_offset = slots.get("a").get.offset
    val x_offset = slots.get("x").get.offset
    result(0).getRefAt(a_offset) should equal(Values.longValue(1))
    result(0).getRefAt(x_offset) should equal(VirtualValues.list(Values.longValue(1), Values.longValue(2), Values.longValue(3), Values.longValue(4)))
  }

  test("should support node values on rhs") {

    // given
    val lhs = createLhs(1)
    val rhs = createRhsWithNumberOfNodes(2)
    val yOffset = rhs.slots.getLongOffsetFor("y")
    val pipe = RollUpApplySlottedPipe(lhs, rhs, collectionRefSlotOffset,
      identifierToCollect = ("y" -> NodeFromSlot(yOffset)),
      nullableIdentifiers = Set("a"), slots)()

    val queryContext = Mockito.mock(classOf[QueryContext])
    val node0 = new FakeNode {
      override def getId(): Long = 0L
    }
    val node1 = new FakeNode {
      override def getId(): Long = 1L
    }
    val nodeOps = Mockito.mock(classOf[Operations[NodeValue]])
    when(queryContext.nodeOps).thenReturn(nodeOps)
    when(nodeOps.getById(0)).thenReturn(fromNodeProxy(node0))
    when(nodeOps.getById(1)).thenReturn(fromNodeProxy(node1))
    val queryState = QueryStateHelper.emptyWith(query = queryContext)

    // when
    val result = pipe.createResults(queryState).toList

    // then
    val a_offset = slots.get("a").get.offset
    val x_offset = slots.get("x").get.offset
    result.head.getRefAt(a_offset) should equal(Values.longValue(1))
    result.head.getRefAt(x_offset) should equal(VirtualValues.list(
      fromNodeProxy(node0),
      fromNodeProxy(node1)))
  }

  test("should set the QueryState when calling down to the RHS") {
    // given
    val lhs = createLhs(1)
    val rhs = mock[Pipe]
    when(rhs.createResults(any())).then(new Answer[Iterator[ExecutionContext]] {
      override def answer(invocation: InvocationOnMock) = {
        val state = invocation.getArguments.apply(0).asInstanceOf[QueryState]
        state.initialContext should not be empty
        Iterator.empty
      }
    })
    val pipe = RollUpApplySlottedPipe(lhs, rhs, collectionRefSlotOffset,
      identifierToCollect = ("y" -> ReferenceFromSlot(0)),
      nullableIdentifiers = Set("a"), slots)()

    // when
    pipe.createResults(QueryStateHelper.empty).toList

    // then should not throw exception
  }

  private def createRhs(data: Any*) = {
    val rhsData = data.map { case v => Map("y" -> v) }
    val rhsPipeline = slots.copy()
      .newReference("y", nullable = false, CTNumber)
    new FakeSlottedPipe(rhsData.iterator, rhsPipeline)
  }

  private def createRhsWithNumberOfNodes(numberOfNodes: Int) = {
    val rhsData = for (i <- 0 until numberOfNodes) yield Map("y" -> i)
    val rhsPipeline = slots.copy()
      .newLong("y", nullable = false, CTNode)
    new FakeSlottedPipe(rhsData.iterator, rhsPipeline)
  }

  private def createLhs(data: Any*) = {
    val lhsData = data.map { case v => Map("a" -> v) }
    new FakeSlottedPipe(lhsData.iterator, slots)
  }
}

