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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationBuilder
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ApplySlottedPipeTest extends CypherFunSuite {

  test("Close should close current RHS and LHS.") {
    val slots = SlotConfigurationBuilder.empty
      .newLong("a", nullable = false, CTNode)
      .newLong("b", nullable = false, CTNode)
      .build()

    val lhs = FakeSlottedPipe(Seq(Map("a" -> 10), Map("a" -> 11)), slots)
    val rhs = FakeSlottedPipe(Seq(Map("b" -> 20), Map("b" -> 21)), slots)
    val pipe = ApplySlottedPipe(lhs, rhs)()
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
