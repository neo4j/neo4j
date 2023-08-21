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

class DistinctPipeTest extends CypherFunSuite {

  test("should be lazy") {
    val input = new FakePipe(Seq(Map("a" -> 10), Map("a" -> 11), Map("a" -> 12), Map("a" -> 13)))
    val pipe = DistinctPipe(input, Array(DistinctPipe.GroupingCol("a", Variable("a"))))()
    // when
    val res = pipe.createResults(QueryStateHelper.emptyWithValueSerialization)
    res.next()
    // then
    input.numberOfPulledRows should be <= 2 // We use a prefetching iterator, so we fetch one extra row
  }

  test("exhaust should close seen set") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)

    val input = new FakePipe(Seq(Map("a" -> 10), Map("a" -> 11), Map("a" -> 12), Map("a" -> 13)))
    val pipe = DistinctPipe(input, Array(DistinctPipe.GroupingCol("a", Variable("a"))))()
    // exhaust
    pipe.createResults(QueryStateHelper.emptyWithResourceManager(resourceManager)).toList
    input.wasClosed shouldBe true
    monitor.closedResources.collect { case t: DistinctSet[_] => t } should have size (1)
  }

  test("close should close seen set") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)

    val input = new FakePipe(Seq(Map("a" -> 10), Map("a" -> 11), Map("a" -> 12), Map("a" -> 13)))
    val pipe = DistinctPipe(input, Array(DistinctPipe.GroupingCol("a", Variable("a"))))()
    val result = pipe.createResults(QueryStateHelper.emptyWithResourceManager(resourceManager))
    result.close()
    input.wasClosed shouldBe true
    monitor.closedResources.collect { case t: DistinctSet[_] => t } should have size (1)
  }
}
