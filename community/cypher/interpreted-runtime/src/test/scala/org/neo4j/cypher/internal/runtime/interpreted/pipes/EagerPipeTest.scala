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
import org.neo4j.kernel.impl.util.collection.EagerBuffer

class EagerPipeTest extends CypherFunSuite {

  private val queryState = QueryStateHelper.emptyWithValueSerialization

  test("should be eager") {
    // Given a lazy iterator that is not empty
    val src = FakePipe(Seq.fill(10)(Map()))
    val eager = EagerPipe(src)()

    // When
    val resultIterator = eager.createResults(queryState)

    src.numberOfPulledRows shouldBe 10
    resultIterator should not be empty
  }

  test("close should close buffer") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val input = new FakePipe(Seq(Map("a" -> 10), Map("a" -> null)))
    val pipe = EagerPipe(input)()
    val result = pipe.createResults(QueryStateHelper.emptyWithResourceManager(resourceManager))
    result.close()
    input.wasClosed shouldBe true
    monitor.closedResources.collect { case t: EagerBuffer[_] => t } should have size (1)
  }

  test("exhaust should close buffer") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val input = new FakePipe(Seq(Map("a" -> 10), Map("a" -> null)))
    val pipe = EagerPipe(input)()
    // exhaust
    pipe.createResults(QueryStateHelper.emptyWithResourceManager(resourceManager)).toList
    input.wasClosed shouldBe true
    monitor.closedResources.collect { case t: EagerBuffer[_] => t } should have size (1)
  }
}
