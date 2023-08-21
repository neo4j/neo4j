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
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Variable
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.collection.DistinctSet

class OrderedDistinctPipeTest extends CypherFunSuite {

  test("iterating should close seen sets") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)

    val input = new FakePipe(Seq(
      Map("a" -> 10, "b" -> 20),
      Map("a" -> 10, "b" -> 20),
      Map("a" -> 11, "b" -> 21),
      Map("a" -> 11, "b" -> 21),
      Map("a" -> 12, "b" -> 22),
      Map("a" -> 12, "b" -> 22)
    ))
    val pipe = OrderedDistinctPipe(
      input,
      Array(
        DistinctPipe.GroupingCol("a", Variable("a"), ordered = true),
        DistinctPipe.GroupingCol("b", Variable("b"))
      )
    )()
    val result = pipe.createResults(QueryStateHelper.emptyWithResourceManager(resourceManager))
    monitor.closedResources.collect { case t: DistinctSet[_] => t } should have size 0
    result.next() // a=10, b=20
    monitor.closedResources.collect { case t: DistinctSet[_] => t } should have size 1
    result.next() // a=11, b=21
    monitor.closedResources.collect { case t: DistinctSet[_] => t } should have size 2
    result.next() // a=12, b=22
    monitor.closedResources.collect { case t: DistinctSet[_] => t } should have size 3
    result.hasNext shouldBe false
  }

  test("close should close seen set") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)

    val input = new FakePipe(Seq(
      Map("a" -> 10, "b" -> 20),
      Map("a" -> 10, "b" -> 20),
      Map("a" -> 11, "b" -> 21),
      Map("a" -> 11, "b" -> 21),
      Map("a" -> 12, "b" -> 22),
      Map("a" -> 12, "b" -> 22)
    ))
    val pipe = OrderedDistinctPipe(
      input,
      Array(
        DistinctPipe.GroupingCol("a", Variable("a"), ordered = true),
        DistinctPipe.GroupingCol("b", Variable("b"))
      )
    )()
    val result = pipe.createResults(QueryStateHelper.emptyWithResourceManager(resourceManager))
    result.close()
    input.wasClosed shouldBe true
    monitor.closedResources.collect { case t: DistinctSet[_] => t } should have size (1)
  }
}
