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
package org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.CountStar
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Variable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CommunityCypherRowFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DistinctPipe
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.collection.HeapTrackingOrderedAppendMap
import org.neo4j.values.storable.Values

class GroupingAggTableTest extends CypherFunSuite {

  test("close should close table") {
    // given
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val table = new GroupingAggTable(
      Array(DistinctPipe.GroupingCol("a", Variable("a"))),
      { case (row, _) => row.getByName("a") },
      Array(AggregationPipe.AggregatingCol("c", CountStar())),
      QueryStateHelper.emptyWithResourceManager(resourceManager),
      CommunityCypherRowFactory(),
      Id(0)
    )
    table.clear()

    table.processRow(CypherRow.from("a" -> Values.intValue(1)))
    table.processRow(CypherRow.from("a" -> Values.intValue(1)))
    table.processRow(CypherRow.from("a" -> Values.intValue(2)))
    table.processRow(CypherRow.from("a" -> Values.intValue(2)))

    // when
    val iter = table.result()
    iter.close()

    // then
    monitor.closedResources.collect { case t: HeapTrackingOrderedAppendMap[_, _] => t } should have size 1
  }
}
