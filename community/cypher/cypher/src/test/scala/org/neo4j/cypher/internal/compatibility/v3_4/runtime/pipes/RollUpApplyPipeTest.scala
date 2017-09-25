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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.ValueComparisonHelper.beEquivalentTo
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.frontend.v3_4.symbols._
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.VirtualValues

class RollUpApplyPipeTest extends CypherFunSuite with PipeTestSupport {
  test("when rhs returns nothing, an empty collection should be produced") {
    // given
    val lhs = createLhs(1)
    val rhs = pipeWithResults { (state) => Iterator() }
    val pipe = RollUpApplyPipe(lhs, rhs, collectionName = "x", identifierToCollect = "y", nullableIdentifiers = Set("a"))()

    // when
    val result = pipe.createResults(QueryStateHelper.empty).toList

    // then
    result should beEquivalentTo(List(Map("a" -> 1, "x" -> Seq.empty)))
  }

  test("when rhs has null values on nullableIdentifiers, a null value should be produced") {
    // given
    val lhs = createLhs(null, 1)
    val rhs = pipeWithResults { (state) => Iterator() }
    val pipe = RollUpApplyPipe(lhs, rhs, collectionName = "x", identifierToCollect = "y", nullableIdentifiers = Set("a"))()

    // when
    val result = pipe.createResults(QueryStateHelper.empty).toList

    // then
    result should equal(List(
      Map("a" -> NO_VALUE, "x" -> NO_VALUE),
      Map("a" -> Values.intValue(1), "x" -> VirtualValues.EMPTY_LIST)))
  }

  test("when rhs produces multiple rows with values, they are turned into a collection") {
    // given
    val lhs = createLhs(1)
    val rhs = createRhs(1, 2, 3, 4)
    val pipe = RollUpApplyPipe(lhs, rhs, collectionName = "x", identifierToCollect = "y", nullableIdentifiers = Set("a"))()

    // when
    val result = pipe.createResults(QueryStateHelper.empty).toList

    // then
    result should beEquivalentTo(List(
      Map("a" -> 1, "x" -> Seq(1, 2, 3, 4))))
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
    val pipe = RollUpApplyPipe(lhs, rhs, collectionName = "x", identifierToCollect = "y", nullableIdentifiers = Set("a"))()

    // when
    pipe.createResults(QueryStateHelper.empty).toList

    // then should not throw exception
  }

  private def createRhs(data: Any*) = {
    val rhsData = data.map { case v => Map("y" -> v) }
    new FakePipe(rhsData.iterator, "a" -> CTAny)
  }

  private def createLhs(data: Any*) = {
    val lhsData = data.map { case v => Map("a" -> v) }
    new FakePipe(lhsData.iterator, "a" -> CTNumber)
  }
}
