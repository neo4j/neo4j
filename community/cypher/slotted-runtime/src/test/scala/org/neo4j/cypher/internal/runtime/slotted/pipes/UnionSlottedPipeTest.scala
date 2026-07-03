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
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class UnionSlottedPipeTest extends CypherFunSuite {

  test("close should close rhs and lhs when exhausted") {
    val slots = SlotConfigurationBuilder.empty
      .newLong("a", nullable = false, CTNode)
      .newLong("b", nullable = false, CTNode)
      .build()
    val lhs = FakeSlottedPipe(Seq(Map("a" -> 10), Map("a" -> 11)), slots)
    val rhs = FakeSlottedPipe(Seq(Map("b" -> 20), Map("b" -> 21)), slots)
    val mapping = SlottedPipeMapper.computeUnionRowMapping(slots, slots)
    val pipe = UnionSlottedPipe(lhs, rhs, slots, mapping, mapping)()
    val result = pipe.createResults(QueryStateHelper.empty)
    result.next()
    result.next()
    result.next()
    result.next()
    result.hasNext shouldBe false

    lhs.wasClosed shouldBe true
    rhs.wasClosed shouldBe true
    lhs.createCount shouldBe 1
    rhs.createCount shouldBe 1
  }

  test("close should close rhs and lhs") {
    val slots = SlotConfigurationBuilder.empty
      .newLong("a", nullable = false, CTNode)
      .newLong("b", nullable = false, CTNode)
      .build()

    val lhs = FakeSlottedPipe(Seq(Map("a" -> 10), Map("a" -> 11)), slots)
    val rhs = FakeSlottedPipe(Seq(Map("b" -> 20), Map("b" -> 21)), slots)
    val mapping = SlottedPipeMapper.computeUnionRowMapping(slots, slots)
    val pipe = UnionSlottedPipe(lhs, rhs, slots, mapping, mapping)()
    val result = pipe.createResults(QueryStateHelper.empty)
    result.next()
    result.next()
    result.next()
    result.hasNext shouldBe true

    lhs.wasClosed shouldBe true
    rhs.wasClosed shouldBe false

    result.close()

    lhs.wasClosed shouldBe true
    rhs.wasClosed shouldBe true
    lhs.createCount shouldBe 1
    rhs.createCount shouldBe 1
  }

  test("close should not close rhs if it's not used") {
    val slots = SlotConfigurationBuilder.empty
      .newLong("a", nullable = false, CTNode)
      .newLong("b", nullable = false, CTNode)
      .build()

    val lhs = FakeSlottedPipe(Seq(Map("a" -> 10), Map("a" -> 11)), slots)
    val rhs = FakeSlottedPipe(Seq(Map("b" -> 20), Map("b" -> 21)), slots)
    val mapping = SlottedPipeMapper.computeUnionRowMapping(slots, slots)
    val pipe = UnionSlottedPipe(lhs, rhs, slots, mapping, mapping)()
    val result = pipe.createResults(QueryStateHelper.empty)
    result.next()
    result.next()
    result.close()

    lhs.wasClosed shouldBe true
    lhs.createCount shouldBe 1
    rhs.createCount shouldBe 0
  }
}
