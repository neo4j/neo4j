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
import org.neo4j.cypher.internal.runtime.slotted.Ascending
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContextOrdering
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class OrderedUnionSlottedPipeTest extends CypherFunSuite {

  test("Close should close RHS and LHS.") {
    val slots = SlotConfigurationBuilder.empty
      .newLong("a", nullable = false, CTNode)
      .build()

    val lhs = FakeSlottedPipe(Seq(Map("a" -> 10), Map("a" -> 11), Map("a" -> 25)), slots)
    val rhs = FakeSlottedPipe(Seq(Map("a" -> 20), Map("a" -> 21), Map("a" -> 26)), slots)
    val mapping = SlottedPipeMapper.computeUnionRowMapping(slots, slots)
    val pipe = OrderedUnionSlottedPipe(
      lhs,
      rhs,
      slots,
      mapping,
      mapping,
      SlottedExecutionContextOrdering.asComparator(List(Ascending(slots("a").slot)))
    )()
    val result = pipe.createResults(QueryStateHelper.empty)
    result.next()
    result.close()

    lhs.wasClosed shouldBe true
    rhs.wasClosed shouldBe true
  }
}
