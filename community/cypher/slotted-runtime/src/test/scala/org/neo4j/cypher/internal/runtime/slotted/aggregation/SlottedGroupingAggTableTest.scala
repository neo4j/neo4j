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
package org.neo4j.cypher.internal.runtime.slotted.aggregation

import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationBuilder
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.CountStar
import org.neo4j.cypher.internal.runtime.slotted.expressions.ReferenceFromSlot
import org.neo4j.cypher.internal.runtime.slotted.pipes.FakeSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.SlotExpression
import org.neo4j.cypher.internal.runtime.slotted.pipes.SlottedGroupingExpression1
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.collection.HeapTrackingOrderedAppendMap

class SlottedGroupingAggTableTest extends CypherFunSuite {

  test("close should close table") {
    // given
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val slots = SlotConfigurationBuilder.empty
      .newReference("a", nullable = false, CTInteger)
      .newReference("c", nullable = false, CTInteger)
      .build()
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)
    val table = new SlottedGroupingAggTable(
      slots,
      SlottedGroupingExpression1(SlotExpression(slots("a").slot, ReferenceFromSlot(0))),
      Map(slots("c").offset -> CountStar()),
      state,
      Id(0),
      slots.size()
    )
    table.clear()

    val input =
      FakeSlottedPipe(Seq(Map("a" -> 1), Map("a" -> 1), Map("a" -> 2), Map("a" -> 2)), slots).createResults(state)
    table.processRow(input.next())
    table.processRow(input.next())
    table.processRow(input.next())
    table.processRow(input.next())

    // when
    val iter = table.result()
    iter.close()

    // then
    monitor.closedResources.collect { case t: HeapTrackingOrderedAppendMap[_, _] => t } should have size 1
  }
}
