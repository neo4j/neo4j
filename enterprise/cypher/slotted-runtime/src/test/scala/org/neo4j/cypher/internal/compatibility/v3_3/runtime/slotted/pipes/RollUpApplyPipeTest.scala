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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.VirtualValues

class RollUpApplySlottedPipeTest extends CypherFunSuite with PipeTestSupport {
  val pipeline = PipelineInformation
    .empty
    .newReference("a", nullable = true, CTNumber)
    .newReference("y", nullable = true, CTNumber)
    .newReference("x", nullable = false, CTList(CTNumber)) // TODO: If you exchange the order of x and y an error will occur in test 3. Still okay?

  test("when rhs returns nothing, an empty collection should be produced") {
    // given
    val lhs = createLhs(1)
    val rhs = pipeWithResults { (state) => Iterator() }
    val pipe = RollUpApplySlottedPipe(lhs, rhs, collectionName = "x", identifierToCollect = "y", nullableIdentifiers = Set("a"), pipeline)()

    // when
    val result = pipe.createResults(QueryStateHelper.empty).toList

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
    val pipe = RollUpApplySlottedPipe(lhs, rhs, collectionName = "x", identifierToCollect = "y", nullableIdentifiers = Set("a"), pipeline)()

    // when
    val result = pipe.createResults(QueryStateHelper.empty).toList

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
    val pipe = RollUpApplySlottedPipe(lhs, rhs, collectionName = "x", identifierToCollect = "y", nullableIdentifiers = Set("a"), pipeline)()

    // when
    val result = pipe.createResults(QueryStateHelper.empty).toList

    // then
    val a_offset = pipeline.get("a").get.offset
    val x_offset = pipeline.get("x").get.offset
    result(0).getRefAt(a_offset) should equal(Values.longValue(1))
    result(0).getRefAt(x_offset) should equal(VirtualValues.list(Values.longValue(1), Values.longValue(2), Values.longValue(3), Values.longValue(4)))

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
    val pipe = RollUpApplySlottedPipe(lhs, rhs, collectionName = "x", identifierToCollect = "y", nullableIdentifiers = Set("a"), pipeline)()

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

  private def createLhs(data: Any*) = {
    val lhsData = data.map { case v => Map("a" -> v) }
    val pipeline = PipelineInformation
      .empty
      .newReference("a", nullable = true, CTNumber)
    new FakeSlottedPipe(lhsData.iterator, pipeline)
  }
}

