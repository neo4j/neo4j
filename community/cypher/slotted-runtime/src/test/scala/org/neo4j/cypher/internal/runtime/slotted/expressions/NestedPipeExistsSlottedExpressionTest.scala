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
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationBuilder
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.slotted.pipes.FakeSlottedPipe
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NestedPipeExistsSlottedExpressionTest extends CypherFunSuite {

  test("Should close pipe results.") {
    // given
    val state = QueryStateHelper.empty
    val outerSlots = SlotConfigurationBuilder.empty.newLong("x", nullable = false, CTNode).build()
    val innerSlots = SlotConfigurationBuilder.empty.newLong("a", nullable = false, CTNode).build()
    val input = FakeSlottedPipe(Seq(Map("a" -> 10), Map("a" -> 11)), innerSlots)
    val npee = NestedPipeExistsSlottedExpression(input, innerSlots, Array(), Id(0))
    // when
    val outerRow = FakeSlottedPipe(Seq(Map("x" -> 42)), outerSlots).createResults(state).next()
    npee.apply(outerRow, state)
    // then
    input.wasClosed shouldBe true
  }
}
