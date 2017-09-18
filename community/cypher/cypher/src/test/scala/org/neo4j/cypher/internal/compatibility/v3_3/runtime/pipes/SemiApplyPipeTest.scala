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

import org.neo4j.cypher.ValueComparisonHelper.beEquivalentTo
import org.neo4j.cypher.internal.frontend.v3_4.symbols.CTNumber
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.intValue

class SemiApplyPipeTest extends CypherFunSuite with PipeTestSupport {

  test("should only let through the one that matches") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val rhs = pipeWithResults((state: QueryState) => {
        val initialContext = state.initialContext.get
        if (initialContext("a") == intValue(1)) Iterator(initialContext) else Iterator.empty
      })

    val result = SemiApplyPipe(lhs, rhs, negated = false)().createResults(QueryStateHelper.empty).toList

    result should beEquivalentTo(List(Map("a" -> 1)))
  }

  test("should only let through the one that not matches when negated") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val rhs = pipeWithResults((state: QueryState) => {
        val initialContext = state.initialContext.get
        if (initialContext("a") == intValue(1)) Iterator(initialContext) else Iterator.empty
      })

    val result = SemiApplyPipe(lhs, rhs, negated = true)().createResults(QueryStateHelper.empty).toList

    result should beEquivalentTo(List(Map("a" -> 2)))
  }

  test("should not let anything through if rhs is empty") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)
    val rhs = new FakePipe(Iterator.empty)

    val result = SemiApplyPipe(lhs, rhs, negated = false)().createResults(QueryStateHelper.empty).toList

    result should be(empty)
  }

  test("should let everything through if rhs is empty and negated") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)
    val rhs = new FakePipe(Iterator.empty)

    val result = SemiApplyPipe(lhs, rhs, negated = true)().createResults(QueryStateHelper.empty).toList

    result should beEquivalentTo(lhsData)
  }

  test("should let everything through if rhs is nonEmpty") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)
    val rhs = new FakePipe(Iterator(Map("a" -> 1)))

    val result = SemiApplyPipe(lhs, rhs, negated = false)().createResults(QueryStateHelper.empty).toList

    result should beEquivalentTo(lhsData)
  }

  test("should let nothing through if rhs is nonEmpty and negated") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)
    val rhs = new FakePipe(Iterator(Map("a" -> 1)))

    val result = SemiApplyPipe(lhs, rhs, negated = true)().createResults(QueryStateHelper.empty).toList

    result should be(empty)
  }

  test("if lhs is empty, rhs should not be touched regardless if it is negated or not") {
    val rhs = pipeWithResults((_) => fail("should not use this"))

    val lhs = new FakePipe(Iterator.empty)

    // Should not throw
    SemiApplyPipe(lhs, rhs, negated = false)().createResults(QueryStateHelper.empty).toList
  }
}
