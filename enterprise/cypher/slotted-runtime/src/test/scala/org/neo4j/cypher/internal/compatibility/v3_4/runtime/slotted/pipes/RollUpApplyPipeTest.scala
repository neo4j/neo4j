/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.pipes

import org.mockito.Matchers._
import org.mockito.Mockito
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions.FakeEntityTestSupport
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.{Pipe, PipeTestSupport, QueryState, QueryStateHelper}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.expressions.{NodeFromSlot, ReferenceFromSlot}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.cypher.internal.frontend.v3_4.symbols._
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.spi.v3_4.{Operations, QueryContext}
import org.neo4j.graphdb.Node
import org.neo4j.helpers.ValueUtils
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.VirtualValues

class RollUpApplySlottedPipeTest extends CypherFunSuite with PipeTestSupport with FakeEntityTestSupport {
  val pipeline = PipelineInformation
    .empty
    .newReference("a", nullable = true, CTNumber)
    .newReference("x", nullable = false, CTList(CTNumber)) // NOTE: This has to be last since that is the order in which the slots are assumed to be allocated

  test("when rhs returns nothing, an empty collection should be produced") {
    // given
    val lhs = createLhs(1)
    val rhs = pipeWithResults { (state) => Iterator() }
    val pipe = RollUpApplySlottedPipe(lhs, rhs, collectionName = "x",
      identifierToCollect = ("y" -> ReferenceFromSlot(0)),
      nullableIdentifiers = Set("a"), pipeline)()

    // when
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList

    // then
    val a_offset = pipeline.get("a").get.offset
    val x_offset = pipeline.get("x").get.offset
    result.head.getRefAt(a_offset) should equal(Values.longValue(1))
    result.head.getRefAt(x_offset) should equal(VirtualValues.EMPTY_LIST)
  }

  test("when rhs has null values on nullableIdentifiers, a null value should be produced") {
    // given
    val lhs = createLhs(null, 1)
    val rhs = pipeWithResults { (state) => Iterator() }
    val pipe = RollUpApplySlottedPipe(lhs, rhs, collectionName = "x",
      identifierToCollect = ("y" -> ReferenceFromSlot(0)),
      nullableIdentifiers = Set("a"), pipeline)()

    // when
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList

    // then
    val a_offset = pipeline.get("a").get.offset
    val x_offset = pipeline.get("x").get.offset
    result(0).getRefAt(a_offset) should equal(NO_VALUE)
    result(0).getRefAt(x_offset) should equal(NO_VALUE)
    result(1).getRefAt(a_offset) should equal(Values.intValue(1))
    result(1).getRefAt(x_offset) should equal(VirtualValues.EMPTY_LIST)
  }

  test("when rhs produces multiple rows with values, they are turned into a collection") {
    // given
    val lhs = createLhs(1)
    val rhs = createRhs(1, 2, 3, 4)
    val yOffset = rhs.pipeline.getReferenceOffsetFor("y")
    val pipe = RollUpApplySlottedPipe(lhs, rhs, collectionName = "x",
      identifierToCollect = ("y" -> ReferenceFromSlot(yOffset)),
      nullableIdentifiers = Set("a"), pipeline)()

    // when
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList

    // then
    val a_offset = pipeline.get("a").get.offset
    val x_offset = pipeline.get("x").get.offset
    result(0).getRefAt(a_offset) should equal(Values.longValue(1))
    result(0).getRefAt(x_offset) should equal(VirtualValues.list(Values.longValue(1), Values.longValue(2), Values.longValue(3), Values.longValue(4)))
  }

  test("should support node values on rhs") {

    // given
    val lhs = createLhs(1)
    val rhs = createRhsWithNumberOfNodes(2)
    val yOffset = rhs.pipeline.getLongOffsetFor("y")
    val pipe = RollUpApplySlottedPipe(lhs, rhs, collectionName = "x",
      identifierToCollect = ("y" -> NodeFromSlot(yOffset)),
      nullableIdentifiers = Set("a"), pipeline)()

    val queryContext = Mockito.mock(classOf[QueryContext])
    val node0 = new FakeNode {
      override def getId(): Long = 0L
    }
    val node1 = new FakeNode {
      override def getId(): Long = 1L
    }
    val nodeOps = Mockito.mock(classOf[Operations[Node]])
    when(queryContext.nodeOps).thenReturn(nodeOps)
    when(nodeOps.getById(0)).thenReturn(node0)
    when(nodeOps.getById(1)).thenReturn(node1)
    val queryState = QueryStateHelper.emptyWith(query = queryContext)

    // when
    val result = pipe.createResults(queryState).toList

    // then
    val a_offset = pipeline.get("a").get.offset
    val x_offset = pipeline.get("x").get.offset
    result(0).getRefAt(a_offset) should equal(Values.longValue(1))
    result(0).getRefAt(x_offset) should equal(VirtualValues.list(
      ValueUtils.fromNodeProxy(node0),
      ValueUtils.fromNodeProxy(node1)))
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
    val pipe = RollUpApplySlottedPipe(lhs, rhs, collectionName = "x",
      identifierToCollect = ("y" -> ReferenceFromSlot(0)),
      nullableIdentifiers = Set("a"), pipeline)()

    // when
    pipe.createResults(QueryStateHelper.empty).toList

    // then should not throw exception
  }

  private def createRhs(data: Any*) = {
    val rhsData = data.map { case v => Map("y" -> v) }
    val pipeline = PipelineInformation
      .empty
      .newReference("a", nullable = true, CTNumber)
      .newReference("y", nullable = false, CTNumber)
    new FakeSlottedPipe(rhsData.iterator, pipeline)
  }

  private def createRhsWithNumberOfNodes(numberOfNodes: Int) = {
    val rhsData = for (i <- 0 until numberOfNodes) yield Map("y" -> i)
    val pipeline = PipelineInformation
      .empty
      .newReference("a", nullable = true, CTNumber)
      .newLong("y", nullable = false, CTNode)
    new FakeSlottedPipe(rhsData.iterator, pipeline)
  }

  private def createLhs(data: Any*) = {
    val lhsData = data.map { case v => Map("a" -> v) }
    val pipeline = PipelineInformation
      .empty
      .newReference("a", nullable = true, CTNumber)
    new FakeSlottedPipe(lhsData.iterator, pipeline)
  }
}

