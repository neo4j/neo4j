/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.memory.HeapEstimator.shallowSizeOfObjectArray
import org.neo4j.memory.LocalMemoryTracker
import org.neo4j.memory.Measurable
import org.neo4j.memory.MemoryLimitExceededException
import org.neo4j.memory.MemoryPools
import org.neo4j.values.storable.Values

class BoundedQueryMemoryTrackerTest extends CypherFunSuite {

  case class IMem(i: Int) extends Measurable {
    override def estimatedHeapUsage: Long = i
  }

  case class TestMemoryTracker(maxBytes: Long = Long.MaxValue) extends LocalMemoryTracker(MemoryPools.NO_TRACKING, maxBytes, 0, null)

  private val sizeOfGrowingArray: Long = shallowSizeOfObjectArray(GrowingArray.DEFAULT_SIZE) + GrowingArray.SHALLOW_SIZE

  test("Tracks overall memory high water mark with bytes") {
    // Given
    val tracker = BoundedQueryMemoryTracker(TestMemoryTracker())
    val tracker0 = tracker.memoryTrackerForOperator(0)

    // When
    tracker0.allocateHeap(10)
    tracker0.allocateHeap(5)
    tracker0.releaseHeap(6)
    tracker0.allocateHeap(4)

    // Then
    tracker.totalAllocatedMemory should be(15L + sizeOfGrowingArray)
    tracker.maxMemoryOfOperator(0) should be(15L)
    tracker0.heapHighWaterMark() should be(15L)
  }

  test("Tracks overall memory high water mark with AnyValues") {
    // Given
    val tracker = BoundedQueryMemoryTracker(TestMemoryTracker())
    val tracker0 = tracker.memoryTrackerForOperator(0)

    val v1 = Values.intValue(5)
    val v2 = Values.booleanValue(true)
    val v3 = Values.stringValue("foo")
    val v4 = Values.booleanValue(false)

    // When
    tracker0.allocateHeap(v1.estimatedHeapUsage)
    tracker0.allocateHeap(v2.estimatedHeapUsage)
    tracker0.releaseHeap(v3.estimatedHeapUsage)
    tracker0.allocateHeap(v4.estimatedHeapUsage)

    // Then
    val expected = Math.max(v1.estimatedHeapUsage() + v2.estimatedHeapUsage(), v1.estimatedHeapUsage() + v2.estimatedHeapUsage() - v3.estimatedHeapUsage() + v4.estimatedHeapUsage())
    tracker.totalAllocatedMemory should be(expected + sizeOfGrowingArray)
    tracker.maxMemoryOfOperator(0) should be(expected)
    tracker0.heapHighWaterMark() should be(expected)
  }

  test("Throws exception if memory exceeds threshold") {
    // Given
    val tracker = BoundedQueryMemoryTracker(TestMemoryTracker(20 + sizeOfGrowingArray))

    // When
    tracker.allocateHeap(10)
    tracker.allocateHeap(5)
    tracker.releaseHeap(6)
    tracker.allocateHeap(9)

    // Then
    a[MemoryLimitExceededException] should be thrownBy tracker.allocateHeap(3)
  }

  test("Tracks individual memory per operator plus overall") {
    // Given
    val tracker = BoundedQueryMemoryTracker(TestMemoryTracker())
    val tracker0 = tracker.memoryTrackerForOperator(0)
    val tracker1 = tracker.memoryTrackerForOperator(1)
    val tracker2 = tracker.memoryTrackerForOperator(2)
    val iterator0 = Iterator(IMem(1), IMem(6), IMem(8))
    val iterator1 = Iterator(IMem(2), IMem(4), IMem(7))
    val iterator2 = Iterator(IMem(3), IMem(13), IMem(2))

    // When
    tracker0.allocateHeap(iterator0.next().estimatedHeapUsage) // [1, 0, 0] / 1
    tracker2.allocateHeap(10) // [1, 0, 10] / 11
    tracker1.allocateHeap(iterator1.next().estimatedHeapUsage) // [1, 2, 10] / 13
    tracker0.allocateHeap(iterator0.next().estimatedHeapUsage) // [7, 2, 10] / 19
    tracker0.releaseHeap(5) // [2, 2, 10] / 14
    tracker2.allocateHeap(iterator2.next().estimatedHeapUsage) // [2, 2, 13] / 17
    tracker2.allocateHeap(iterator2.next().estimatedHeapUsage) // [2, 2, 26] / 30
    tracker2.allocateHeap(iterator2.next().estimatedHeapUsage) // [2, 2, 28] / 32
    tracker2.releaseHeap(21) // [2, 2, 7] / 11
    tracker0.allocateHeap(iterator0.next().estimatedHeapUsage) // [10, 2, 7] / 19
    tracker1.allocateHeap(iterator1.next().estimatedHeapUsage) // [10, 6, 7] / 23
    tracker1.releaseHeap(2) // [10, 4, 7] / 21
    tracker1.allocateHeap(iterator1.next().estimatedHeapUsage) // [10, 11, 7] / 28

    // Then
    tracker.totalAllocatedMemory should be(32L + sizeOfGrowingArray)
    tracker.maxMemoryOfOperator(0) should be(10L)
    tracker.maxMemoryOfOperator(1) should be(11L)
    tracker.maxMemoryOfOperator(2) should be(28L)
    tracker0.heapHighWaterMark() should be (10L)
    tracker1.heapHighWaterMark() should be (11L)
    tracker2.heapHighWaterMark() should be (28L)
  }
}
