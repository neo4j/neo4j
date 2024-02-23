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

import org.neo4j.collection.trackable.HeapTrackingArrayList
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.ImplicitDummyPos
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.query.QueryExecution
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.TestQueryExecution
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.HeapEstimator.shallowSizeOfInstance
import org.neo4j.memory.LocalMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.util.AnyValueConversions
import org.neo4j.util.Table
import org.neo4j.values.storable.IntValue

class RunQueryAtPipeTest
    extends CypherFunSuite
    with PipeTestSupport
    with ImplicitDummyPos
    with AnyValueConversions {

  test("iterator with same batch size as remote table") {
    val t = Table
      .hdr("a")
      .row(1)
      .row(2)

    val iter = getIterator(t, batchSize = 2)

    iter.toList shouldBe t.asRows
  }

  test("iterator with smaller batch size than remote table") {
    val t = Table
      .hdr("a")
      .row(1)
      .row(2)

    val iter = getIterator(t, batchSize = 1)

    iter.toList shouldBe t.asRows
  }

  test("iterator with larger batch size than remote table") {
    val t = Table
      .hdr("a")
      .row(1)
      .row(2)

    val iter = getIterator(t, batchSize = 3)

    iter.toList shouldBe t.asRows
  }

  test("columns returned in the correct order") {
    val t = Table
      .hdr("a", "b", "c")
      .row(1, 2, 3)

    val iter = getIterator(t)

    iter.toList shouldBe t.asRows
  }

  test("error rethrown by iterator") {
    val err = new Exception("Inner exception")
    val iter = getIterator(TestQueryExecution.fromThrowable(Seq("a"), err))

    the[Exception] thrownBy iter.toList shouldBe err
  }

  test("memory tracker tracks memory per row") {
    val t = Table
      .hdr("a", "b")
      .row(1, 2)
      .row(3, 4)

    val iterMt = new LocalMemoryTracker()
    val expectedMt = new LocalMemoryTracker()

    val batchSize = 1

    val iter = getIterator(t, batchSize = batchSize, memoryTracker = iterMt)

    expectedMt.allocateHeap(
      shallowSizeOfInstance(classOf[HeapTrackingArrayList[_]]) + HeapEstimator.shallowSizeOfObjectArray(batchSize)
    )
    iterMt.estimatedHeapMemory() shouldBe expectedMt.estimatedHeapMemory()

    iter.next()
    expectedMt.allocateHeap(shallowSizeOfInstance(classOf[IntValue]) * 2)

    iter.next()
    expectedMt.allocateHeap(shallowSizeOfInstance(classOf[IntValue]) * 2)

    iterMt.estimatedHeapMemory() shouldBe expectedMt.estimatedHeapMemory()

    iter.close()
    iterMt.estimatedHeapMemory() shouldBe 0
  }

  def getIterator(
    execution: TestQueryExecution,
    batchSize: Int = 10,
    memoryTracker: MemoryTracker = EmptyMemoryTracker.INSTANCE
  ): RunQueryAtIterator = {
    def getExecution(subscriber: QuerySubscriber): QueryExecution = {
      execution.subscriber = subscriber
      execution
    }

    new RunQueryAtIterator(
      getExecution,
      () => CypherRow.empty,
      expectedFields = execution.fieldNames.length,
      batchSize = batchSize,
      memoryTracker = memoryTracker
    )
  }
}
