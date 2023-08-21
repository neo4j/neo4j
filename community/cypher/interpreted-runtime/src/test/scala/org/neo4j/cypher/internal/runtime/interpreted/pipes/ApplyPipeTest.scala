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

import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.ValueComparisonHelper.beEquivalentTo
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values

class ApplyPipeTest extends CypherFunSuite with PipeTestSupport {

  test("should work by applying the identity operator on the rhs") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator)
    val rhs = pipeWithResults { state => Iterator(state.initialContext.get) }

    val result = ApplyPipe(lhs, rhs)().createResults(QueryStateHelper.empty).toList

    result should beEquivalentTo(lhsData)
  }

  test("should work by applying a  on the rhs") {
    val lhsData = List(Map("a" -> 1, "b" -> 3), Map("a" -> 2, "b" -> 4))
    val lhs = new FakePipe(lhsData.iterator)
    val rhsData = "c" -> Values.intValue(36)
    val rhs = pipeWithResults { state =>
      state.initialContext.get.set(rhsData._1, rhsData._2)
      Iterator(state.initialContext.get)
    }

    val result = ApplyPipe(lhs, rhs)().createResults(QueryStateHelper.empty).toList
    val expected: List[Map[String, Any]] = lhsData.map(_ + rhsData)

    result should beEquivalentTo(expected)
  }

  test("Close should close current RHS and LHS.") {
    val lhs = new FakePipe(Seq(Map("a" -> 10), Map("a" -> 11)))
    val rhs = new FakePipe(Seq(Map("b" -> 20), Map("b" -> 21)))
    val pipe = ApplyPipe(lhs, rhs)()
    val result = pipe.createResults(QueryStateHelper.empty)
    result.next() // First row
    val firstRhs = rhs.currentIterator
    result.next() // Second row
    result.next() // Third row. First RHS should be exhausted and closed by now
    lhs.wasClosed shouldBe false
    firstRhs.wasClosed shouldBe true

    val secondRhs = rhs.currentIterator
    result.next() // Fourth row
    result.hasNext shouldBe false // Make sure to exhaust
    lhs.wasClosed shouldBe true
    secondRhs.wasClosed shouldBe true
  }
}
