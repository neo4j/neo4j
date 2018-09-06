/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.CountStar
import org.neo4j.values.storable.Values.longValue
import org.opencypher.v9_0.util.symbols._
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class EagerAggregationSlottedPrimitivePipeTest extends CypherFunSuite with SlottedPipeTestHelper {
  test("should aggregate count(*) on two grouping columns") {
    val slots = SlotConfiguration.empty
      .newLong("a", nullable = false, CTNode)
      .newLong("b", nullable = false, CTNode)
      .newReference("count(*)", nullable = false, CTInteger)

    def source = FakeSlottedPipe(List(
      Map[String, Any]("a" -> 1, "b" -> 1),
      Map[String, Any]("a" -> 1, "b" -> 1),
      Map[String, Any]("a" -> 1, "b" -> 2),
      Map[String, Any]("a" -> 2, "b" -> 2)), slots)

    val grouping = createReturnItemsFor(slots,"a", "b")
    val aggregation = Map(slots("count(*)").offset -> CountStar())
    def aggregationPipe = EagerAggregationSlottedPrimitivePipe(source, slots, grouping, grouping, aggregation)()

    testableResult(aggregationPipe.createResults(QueryStateHelper.empty), slots) should be(List(
      Map[String, Any]("a" -> 1, "b" -> 1, "count(*)" -> longValue(2)),
      Map[String, Any]("a" -> 1, "b" -> 2, "count(*)" -> longValue(1)),
      Map[String, Any]("a" -> 2, "b" -> 2, "count(*)" -> longValue(1))
    ))
  }

  private def createReturnItemsFor(slots: SlotConfiguration, names: String*): Array[Int] = names.map(k => slots(k).offset).toArray

}
