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
import org.neo4j.cypher.internal.runtime.ImplicitValueConversion.toIntValue
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContextHelper.RichExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.TestableIterator
import org.neo4j.cypher.internal.runtime.interpreted.ValueComparisonHelper.beEquivalentTo
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Variable
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.collection
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.doubleArray
import org.neo4j.values.storable.Values.intArray
import org.neo4j.values.storable.Values.intValue

class ValueHashJoinPipeTest extends CypherFunSuite {

  test("should support simple hash join between two identifiers") {
    // given
    val queryState = QueryStateHelper.emptyWithValueSerialization

    val left = mock[Pipe]
    when(left.createResults(queryState)).thenReturn(rows("a", 1, 2))

    val right = mock[Pipe]
    when(right.createResults(queryState)).thenReturn(rows("b", 2, 3))

    // when
    val result = ValueHashJoinPipe(Variable("a"), Variable("b"), left, right)().createResults(queryState)

    // then
    result.toList should beEquivalentTo(List(Map("a" -> 2, "b" -> 2)))
  }

  test("should handle nulls") {
    // given
    val queryState = QueryStateHelper.emptyWithValueSerialization

    val left = mock[Pipe]
    when(left.createResults(queryState)).thenReturn(rows("a", 1, 2, null))

    val right = mock[Pipe]
    when(right.createResults(queryState)).thenReturn(rows("b", 2, 3, null))

    // when
    val result = ValueHashJoinPipe(Variable("a"), Variable("b"), left, right)().createResults(queryState)

    // then
    result.toList should beEquivalentTo(List(Map("a" -> 2, "b" -> 2)))
  }

  test("should handle multiples on both sides") {
    // given
    val leftSide = ClosingIterator(Iterator(
      row("a" -> 1, "a2" -> 1),
      row("a" -> 1, "a2" -> 2),
      row("a" -> 2, "a2" -> 3),
      row("a" -> 3, "a2" -> 4)
    ))

    val rightSide = ClosingIterator(Iterator(
      row("b" -> 1, "b2" -> 1),
      row("b" -> 2, "b2" -> 2),
      row("b" -> 2, "b2" -> 3),
      row("b" -> 4, "b2" -> 4)
    ))

    val queryState = QueryStateHelper.emptyWithValueSerialization

    val left = mock[Pipe]
    when(left.createResults(queryState)).thenReturn(leftSide)

    val right = mock[Pipe]
    when(right.createResults(queryState)).thenReturn(rightSide)

    // when
    val result = ValueHashJoinPipe(Variable("a"), Variable("b"), left, right)().createResults(queryState)

    // then
    result.map(_.toMap).toSet should equal(Set(
      Map("a" -> intValue(1), "b" -> intValue(1), "a2" -> intValue(1), "b2" -> intValue(1)),
      Map("a" -> intValue(1), "b" -> intValue(1), "a2" -> intValue(2), "b2" -> intValue(1)),
      Map("a" -> intValue(2), "b" -> intValue(2), "a2" -> intValue(3), "b2" -> intValue(2)),
      Map("a" -> intValue(2), "b" -> intValue(2), "a2" -> intValue(3), "b2" -> intValue(3))
    ))
  }

  test("should not fetch results from RHS if LHS is empty") {
    // given
    val queryState = QueryStateHelper.emptyWithValueSerialization

    val left = mock[Pipe]
    when(left.createResults(queryState)).thenReturn(ClosingIterator.empty)

    val right = mock[Pipe]

    // when
    val result = ValueHashJoinPipe(Variable("a"), Variable("b"), left, right)().createResults(queryState)

    // then
    result shouldBe empty
    verify(right, never()).createResults(any())
  }

  test("should not fetch results from RHS if no probe table was built") {
    // given
    val queryState = QueryStateHelper.emptyWithValueSerialization

    val left = mock[Pipe]
    when(left.createResults(queryState)).thenReturn(rows("a", null, null, null))

    val right = mock[Pipe]
    val rhsIter = new TestableIterator(rows("b", 1, 2, 3))
    when(right.createResults(queryState)).thenReturn(rhsIter)

    // when
    val result = ValueHashJoinPipe(Variable("a"), Variable("b"), left, right)().createResults(queryState)

    // then
    result shouldBe empty
    rhsIter.fetched should equal(0)
  }

  test("if RHS is empty, terminate building of the probe map early") {
    // given
    val queryState = QueryStateHelper.emptyWithValueSerialization

    val left = mock[Pipe]

    val lhsIterator = new TestableIterator(rows("a", 1, 2, 3))
    when(left.createResults(queryState)).thenReturn(lhsIterator)

    val right = mock[Pipe]
    when(right.createResults(queryState)).thenReturn(ClosingIterator.empty)

    // when
    val result = ValueHashJoinPipe(Variable("a"), Variable("b"), left, right)().createResults(queryState)

    // then
    result shouldBe empty
    lhsIterator.fetched should equal(0)
  }

  test("should support joining on arrays") {
    // given
    val ints = intArray(Array(1, 2, 3))
    val doubles = doubleArray(Array(1.0, 2.0, 3.0))

    val queryState = QueryStateHelper.emptyWithValueSerialization

    val left = mock[Pipe]
    when(left.createResults(queryState)).thenReturn(rows("a", ints, intArray(Array(2, 3, 4))))

    val right = mock[Pipe]
    when(right.createResults(queryState)).thenReturn(rows("b", doubles, intArray(Array(0, 1, 2))))

    // when
    val result = ValueHashJoinPipe(Variable("a"), Variable("b"), left, right)().createResults(queryState)

    // then
    result.toList should beEquivalentTo(List(Map("a" -> ints, "b" -> doubles)))
  }

  test("exhaust should close table") {
    // given
    val monitor = QueryStateHelper.trackClosedMonitor
    val queryState = QueryStateHelper.emptyWithResourceManager(new ResourceManager(monitor))

    val left = new FakePipe(Seq(Map("a" -> 1), Map("a" -> 2)))
    val right = new FakePipe(Seq(Map("b" -> 1), Map("b" -> 2)))

    // when
    ValueHashJoinPipe(Variable("a"), Variable("b"), left, right)().createResults(queryState).toList

    // then
    monitor.closedResources.collect { case t: collection.ProbeTable[_, _] => t } should have size (1)
  }

  test("close should close table") {
    // given
    val monitor = QueryStateHelper.trackClosedMonitor
    val queryState = QueryStateHelper.emptyWithResourceManager(new ResourceManager(monitor))

    val left = new FakePipe(Seq(Map("a" -> 1), Map("a" -> 2)))
    val right = new FakePipe(Seq(Map("b" -> 1), Map("b" -> 2)))

    // when
    val result = ValueHashJoinPipe(Variable("a"), Variable("b"), left, right)().createResults(queryState)
    result.close()

    // then
    monitor.closedResources.collect { case t: collection.ProbeTable[_, _] => t } should have size (1)
  }

  private def row(values: (String, AnyValue)*) = CypherRow.from(values: _*)

  private def rows(variable: String, values: AnyValue*): ClosingIterator[CypherRow] =
    ClosingIterator(values.map(x => CypherRow.from(variable -> x)).iterator)

}
