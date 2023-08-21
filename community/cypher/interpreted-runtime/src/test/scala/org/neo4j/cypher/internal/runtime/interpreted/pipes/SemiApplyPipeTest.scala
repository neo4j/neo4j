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

import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SemiApplyPipeTest extends CypherFunSuite with PipeTestSupport {

  test("if lhs is empty, rhs should not be touched regardless if it is negated or not") {
    val rhs = pipeWithResults(_ => fail("should not use this"))

    val lhs = new FakePipe(Iterator.empty)

    // Should not throw
    SemiApplyPipe(lhs, rhs)().createResults(QueryStateHelper.empty).toList
  }

  test("SemiApply: Each row should immediately close RHS. Exhaust should close LHS.") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val lhs = new FakePipe(Seq(Map("a" -> 10), Map("a" -> 11)))
    val rhs = new FakePipe(Seq(Map("b" -> 20), Map("b" -> 21)))
    val pipe = SemiApplyPipe(lhs, rhs)()
    val result = pipe.createResults(QueryStateHelper.emptyWithResourceManager(resourceManager))
    result.next() // First row
    lhs.wasClosed shouldBe false
    rhs.wasClosed shouldBe true

    rhs.resetClosed()
    result.next() // Second row
    result.hasNext shouldBe false // Make sure to exhaust
    lhs.wasClosed shouldBe true
    rhs.wasClosed shouldBe true
  }

  test("AntiSemiApply: Each row should immediately close RHS. Exhaust should close LHS.") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val lhs = new FakePipe(Seq(Map("a" -> 10), Map("a" -> 11)))
    val rhs = new FakePipe(Seq(Map("b" -> 20), Map("b" -> 21)))
    val pipe = AntiSemiApplyPipe(lhs, rhs)()
    val result = pipe.createResults(QueryStateHelper.emptyWithResourceManager(resourceManager))
    result.hasNext shouldBe false // Make sure to exhaust
    lhs.wasClosed shouldBe true
    rhs.wasClosed shouldBe true
  }
}
