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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.FakePipe.CountingRelationshipIterator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

class OptionalExpandAllPipeTest extends CypherFunSuite {

  test("exhaust should close relationships iterator") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)
    val nodeValue = VirtualValues.nodeValue(0, "n", Values.stringArray(), VirtualValues.map(Array(), Array()))
    val relIter = new CountingRelationshipIterator(PrimitiveLongHelper.relationshipIteratorFrom((0, 0, 0, 0)))
    Mockito.when(state.query.getRelationshipsForIds(any[Long], any[SemanticDirection], any[Array[Int]])).thenReturn(
      relIter
    )

    val input = FakePipe(Seq(Map("a" -> nodeValue)))
    val pipe = OptionalExpandAllPipe(input, "a", "r", "b", SemanticDirection.OUTGOING, new EagerTypes(Array(0)), None)()
    // exhaust
    pipe.createResults(state).toList
    input.wasClosed shouldBe true
    relIter.wasClosed shouldBe true
  }

  test("close should close  relationships iterator") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val state = QueryStateHelper.emptyWithResourceManager(resourceManager)
    val nodeValue = VirtualValues.nodeValue(0, "n", Values.stringArray(), VirtualValues.map(Array(), Array()))

    val relIter = new CountingRelationshipIterator(PrimitiveLongHelper.relationshipIteratorFrom((0, 0, 0, 0)))
    Mockito.when(state.query.getRelationshipsForIds(any[Long], any[SemanticDirection], any[Array[Int]])).thenReturn(
      relIter
    )
    val input = FakePipe(Seq(Map("a" -> nodeValue)))
    val pipe = OptionalExpandAllPipe(input, "a", "r", "b", SemanticDirection.OUTGOING, new EagerTypes(Array(0)), None)()
    val result = pipe.createResults(state)
    result.hasNext shouldBe true // Need to initialize
    result.close()
    input.wasClosed shouldBe true
    relIter.wasClosed shouldBe true
  }
}
